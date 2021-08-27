"""
  Helper functions for Delta Lake
"""

from typing import Iterator

from delta import DeltaTable
from pyspark.sql import SparkSession

from spark_utils.dataframes.functions import get_dataframe_columns, get_dataframe_partitions
from spark_utils.models.hive_table import HiveTableColumn


def publish_delta_to_hive(spark_session: SparkSession, publish_table_name: str, publish_schema_name: str,
                          data_path: str, refresh: bool = False):
    """
      Generate symlink manifest and create an external table in a Hive Metastore used by a SparkSession provided

    :param spark_session: SparkSession that is configured to use an external Hive Metastore
    :param publish_table_name: Name of a table to publish
    :param publish_schema_name: Name of a schema to publish
    :param data_path: Path to delta table
    :param refresh: Drop existing definition befor running CREATE TABLE
    :return:
    """
    delta_table = DeltaTable.forPath(spark_session, data_path)
    delta_table.generate("symlink_format_manifest")

    (columns, partitions, location) = get_table_info(spark_session, data_path)

    column_str = ','.join(map(lambda t_col: f"{t_col.name} {t_col.type}", columns))
    partition_str = ','.join(map(lambda t_col: f"{t_col.name} {t_col.type}", partitions))
    partition_str = '' if not partition_str else f"PARTITIONED BY ({partition_str})"

    create_table_sql = f"""
    CREATE EXTERNAL TABLE {publish_schema_name}.{publish_table_name} ({column_str})
    {partition_str}
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION '{location}/_symlink_format_manifest/'
    """

    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {publish_schema_name}")
    if refresh:
        spark_session.sql(f"DROP TABLE IF EXISTS {publish_schema_name}.{publish_table_name}")

    spark_session.sql(create_table_sql)

    if partition_str:
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

    table_location = definition_table.where("col_name = 'Location'").head(1)[0]["data_type"]

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

        return [table_col for table_col in cols if table_col.name not in parts_names], parts, table_location

    return cols, [], table_location
