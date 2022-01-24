# MIT License
#
# Copyright (c) 2022 Ecco Sneaks & Data
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
  Helper functions for Delta Lake
"""

from typing import Iterator

from delta import DeltaTable
from pyspark.sql import SparkSession

from spark_utils.dataframes.functions import get_dataframe_columns, get_dataframe_partitions
from spark_utils.models.hive_table import HiveTableColumn


def _generate_table_ddl(*,
                        publish_as_symlink: bool,
                        publish_schema_name: str,
                        publish_table_name: str,
                        column_str: str,
                        partition_str: str,
                        location: str) -> str:
    """
      Generates CREATE ... TABLE  statement for Spark SQL.

    :param publish_as_symlink: Generate CREATE EXTERNAL TABLE with SymlinkTextInputFormat input format
    :param publish_schema_name: Hive schema to publish to.
    :param publish_table_name: Hive table.
    :param column_str: Column list, <col> <type>, comma-separated
    :param partition_str: Partition list, <col> <type>, comma-separated. Must not overlap with column list.
    :param location: Physical data location.
    :return:
    """
    if publish_as_symlink:
        return f"""
        CREATE EXTERNAL TABLE {publish_schema_name}.{publish_table_name} ({column_str})
        {partition_str}
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        LOCATION '{location}/_symlink_format_manifest/'
        """

    return f"""
        CREATE TABLE {publish_schema_name}.{publish_table_name}
        USING delta 
        LOCATION '{location}' 
        """


def publish_delta_to_hive(spark_session: SparkSession,
                          publish_table_name: str,
                          publish_schema_name: str,
                          data_path: str,
                          refresh: bool = False,
                          publish_as_symlink=True):
    """
      Generate symlink manifest and create an external table in a Hive Metastore used by a SparkSession provided

      OR

      Generate simple Hive table linked to abfss path

    :param spark_session: SparkSession that is configured to use an external Hive Metastore
    :param publish_table_name: Name of a table to publish
    :param publish_schema_name: Name of a schema to publish
    :param data_path: Path to delta table
    :param refresh: Drop existing definition befor running CREATE TABLE
    :param publish_as_symlink: Generate symlink format manifest to make table readable from Trino
    :return:
    """

    spark_session.sql(
        f"CREATE SCHEMA IF NOT EXISTS {publish_schema_name} location 'abfss://{data_path[8:].split('/')[0]}/'"
    )
    if refresh:
        spark_session.sql(f"DROP TABLE IF EXISTS {publish_schema_name}.{publish_table_name}")

    delta_table = DeltaTable.forPath(spark_session, data_path)

    (columns, partitions, location) = get_table_info(spark_session, data_path)

    column_str = ','.join(map(lambda t_col: f"{t_col.name} {t_col.type}", columns))
    partition_str = ','.join(map(lambda t_col: f"{t_col.name} {t_col.type}", partitions))
    partition_str = '' if not partition_str else f"PARTITIONED BY ({partition_str})"

    if publish_as_symlink:
        delta_table.generate("symlink_format_manifest")

    create_table_sql = _generate_table_ddl(
        publish_as_symlink=publish_as_symlink,
        publish_schema_name=publish_schema_name,
        publish_table_name=publish_table_name,
        column_str=column_str,
        partition_str=partition_str,
        location=location
    )

    spark_session.sql(create_table_sql)

    if partition_str and publish_as_symlink:
        spark_session.sql(f"""MSCK REPAIR TABLE {publish_schema_name}.{publish_table_name}""")


def get_table_info(spark_session: SparkSession, table_path: str) -> (
        Iterator[HiveTableColumn], Iterator[HiveTableColumn], str):
    """
      Reads columns, partitions and table data location

    :param spark_session: SparkSession
    :param table_path: path to a Delta table
    :return:
    """
    definition_table = spark_session.sql(
        f"describe table extended delta.`{table_path}/`")

    definition_table_rows = list(definition_table.toLocalIterator())

    cols = list(get_dataframe_columns(definition_table_rows))

    parts_names_with_index = list(get_dataframe_partitions(definition_table_rows))
    if parts_names_with_index:
        # partitions must be sorted in the way they were applied
        parts_names_with_index.sort(key=lambda pair: pair[0])
        parts_names = list(map(lambda part: part[1], parts_names_with_index))
        parts = []

        for part in parts_names:
            for table_col in cols:
                if table_col.name == part:
                    parts.append(HiveTableColumn(table_col.name, table_col.type))

        return [table_col for table_col in cols if table_col.name not in parts_names], parts, table_path

    return cols, [], table_path
