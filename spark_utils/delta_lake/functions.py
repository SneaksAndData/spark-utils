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
import re
from typing import Iterator, Optional


from delta import DeltaTable
from pyspark.sql import SparkSession

from spark_utils.dataframes.functions import get_dataframe_columns, get_dataframe_partitions
from spark_utils.models.hive_table import HiveTableColumn


def _generate_table_ddl(
    *,
    publish_as_symlink: bool,
    publish_schema_name: str,
    publish_table_name: str,
    column_str: str,
    partition_str: str,
    location: str,
) -> str:
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


def publish_delta_to_hive(
    spark_session: SparkSession,
    publish_table_name: str,
    publish_schema_name: str,
    data_path: str,
    refresh: bool = False,
    publish_as_symlink=True,
):
    """
      Generate symlink manifest and create an external table in a Hive Metastore used by a SparkSession provided

      OR

      Generate simple Hive table linked to abfss or s3a path

    :param spark_session: SparkSession that is configured to use an external Hive Metastore
    :param publish_table_name: Name of a table to publish
    :param publish_schema_name: Name of a schema to publish
    :param data_path: Path to delta table
    :param refresh: Drop existing definition befor running CREATE TABLE
    :param publish_as_symlink: Generate symlink format manifest to make table readable from Trino
    :return:
    """
    protocol, data_location = re.match(r"^(abfss|s3a):\/\/([^ ]+)$", data_path).groups()
    spark_session.sql(
        f"CREATE SCHEMA IF NOT EXISTS {publish_schema_name} location '{protocol}://{'/'.join(data_location.split('/')[0:-1])}/'"
    )
    if refresh:
        spark_session.sql(f"DROP TABLE IF EXISTS {publish_schema_name}.{publish_table_name}")

    delta_table = DeltaTable.forPath(spark_session, data_path)

    (columns, partitions, location) = get_table_info(spark_session, data_path)

    column_str = ",".join(map(lambda t_col: f"{t_col.name} {t_col.type}", columns))
    partition_str = ",".join(map(lambda t_col: f"{t_col.name} {t_col.type}", partitions))
    partition_str = "" if not partition_str else f"PARTITIONED BY ({partition_str})"

    if publish_as_symlink:
        delta_table.generate("symlink_format_manifest")

    create_table_sql = _generate_table_ddl(
        publish_as_symlink=publish_as_symlink,
        publish_schema_name=publish_schema_name,
        publish_table_name=publish_table_name,
        column_str=column_str,
        partition_str=partition_str,
        location=location,
    )

    spark_session.sql(create_table_sql)

    if partition_str and publish_as_symlink:
        spark_session.sql(f"""MSCK REPAIR TABLE {publish_schema_name}.{publish_table_name}""")


def get_table_info(
    spark_session: SparkSession, table_path: str
) -> (Iterator[HiveTableColumn], Iterator[HiveTableColumn], str):
    """
      Reads columns, partitions and table data location

    :param spark_session: SparkSession
    :param table_path: path to a Delta table
    :return:
    """
    definition_table = spark_session.sql(f"describe table extended delta.`{table_path}/`")

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


def delta_compact(
    spark_session: SparkSession,
    path: str,
    retain_hours: float = 48,
    compact_from_predicate: Optional[str] = None,
    target_file_size_bytes: Optional[int] = None,
    vacuum_only: bool = True,
    refresh_cache=False,
) -> None:
    """
      Runs bin-packing optimization to reduce number of files/increase average file size in the table physical storage.
      Refreshes delta cache after opt/vacuum have been finished.
      https://docs.delta.io/latest/optimizations-oss.html#optimizations

    :param spark_session: Spark session that will perform the operation.
    :param path: Path to delta table, filesystem or hive.
    :param retain_hours: Age of data to retain, defaults to 48 hours.
    :param compact_from_predicate: Optional predicate to select a subset of data to compact (sql string).
    :param target_file_size_bytes: Optional target file size in bytes. Defaults to system default (1gb for Delta 2.1) if not provided.
    :param vacuum_only: If set to True, will perform a vacuum operation w/o compaction.
    :param refresh_cache: Refreshes table cache for this spark session.
    :return:
    """
    spark_session.conf.set("spark.databricks.delta.optimize.repartition.enabled", "true")

    table_to_compact = (
        DeltaTable.forPath(sparkSession=spark_session, path=path)
        if "://" in path
        else DeltaTable.forName(sparkSession=spark_session, tableOrViewName=path)
    )

    if not vacuum_only:
        if target_file_size_bytes:
            spark_session.conf.set("spark.databricks.delta.optimize.minFileSize", str(target_file_size_bytes))
            spark_session.conf.set("spark.databricks.delta.optimize.maxFileSize", str(target_file_size_bytes))

        if compact_from_predicate:
            table_to_compact.optimize().where(compact_from_predicate).executeCompaction()
        else:
            table_to_compact.optimize().executeCompaction()

    table_path = f"delta.`{path}`" if "://" in path else path
    current_interval = int(
        re.search(
            r"\b\d+\b",
            table_to_compact.detail().head().properties.get("delta.logRetentionDuration", "interval 168 hours"),
        ).group()
    )

    if current_interval != round(retain_hours):
        spark_session.sql(
            f"ALTER table {table_path} SET TBLPROPERTIES ('delta.logRetentionDuration'='interval {round(retain_hours)} hours')"
        )

    table_to_compact.vacuum(retentionHours=retain_hours)

    if refresh_cache:
        if "://" in path:
            spark_session.sql(f"refresh {path}")
        else:
            spark_session.sql(f"refresh table {path}")
