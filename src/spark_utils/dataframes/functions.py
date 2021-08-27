"""
  Helper functions for Spark Dataframes
"""

from typing import List, Optional, Iterator, Tuple

from hadoop_fs_wrapper.wrappers.file_system import FileSystem
from pyspark import Row
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.functions import lit, col

from spark_utils.dataframes.sets.functions import case_insensitive_diff
from spark_utils.models.hive_table import HiveTableColumn
from spark_utils.models.job_socket import JobSocket


def empty_column(col_name: str) -> Column:
    """
      Returns an empty (null values) column
    :param col_name: Column name
    :return: Spark SQL column
    """
    return lit(None).alias(col_name)


def order_df_and_add_missing_cols(source_df: DataFrame, columns_order_list: List[str],
                                  df_missing_fields: List[str]) -> DataFrame:
    """ return ordered dataFrame by the columns order list with null in missing columns """
    if not df_missing_fields:  # no missing fields for the df
        return source_df.select(columns_order_list)

    columns = []
    missing_lower = list(map(lambda x: x.lower(), df_missing_fields))
    for col_name in columns_order_list:
        if col_name.lower() not in missing_lower:
            columns.append(col_name)
        else:
            columns.append(empty_column(col_name))
    return source_df.select(columns)


def add_missing_columns(source_df: DataFrame, missing_column_names: List[str]) -> DataFrame:
    """
      Add missing columns as null in the end of the columns list

    :param source_df: source Dataframe
    :param missing_column_names: Columns to add
    :return: Dataframe with new columns from missing_column_names, initialized with nulls
    """
    new_df = None
    for missing_col in missing_column_names:
        new_df = source_df.withColumn(missing_col, empty_column(missing_col))
    return new_df


def order_and_union_dataframes(left_df: DataFrame, right_df: DataFrame, left_list_miss_cols: List[str],
                               right_list_miss_cols: List[str]) -> DataFrame:
    """
      Union data frames and order columns by left_df.

    :param left_df: Left dataframe
    :param right_df: Right dataframe
    :param left_list_miss_cols: Columns to add to the left df
    :param right_list_miss_cols: Columns to add to the right df
    :return:
    """
    left_df_all_cols = add_missing_columns(left_df, left_list_miss_cols)
    right_df_all_cols = order_df_and_add_missing_cols(right_df, left_df_all_cols.schema.names,
                                                      right_list_miss_cols)
    return left_df_all_cols.union(right_df_all_cols)


def union_dataframes(left_df: DataFrame, right_df: DataFrame) -> DataFrame:
    """
      Union between two dataFrames, if there is a gap of column fields,
      it will append all missing columns as nulls

    :param left_df: Left dataframe
    :param right_df: Right dataframe
    :return: A new dataframe, union of left and right
    """
    # Check for None input
    assert left_df, 'left_df parameter should not be None'
    assert right_df, 'right_df parameter should not be None'

    # For data frames with equal columns and order - regular union
    if left_df.schema.names == right_df.schema.names:
        return left_df.union(right_df)

    # Different columns
    # Save dataFrame columns name list as set
    left_df_col_list = set(left_df.schema.names)
    right_df_col_list = set(right_df.schema.names)
    # Diff columns between left_df and right_df
    right_list_miss_cols = case_insensitive_diff(
        left_df_col_list, right_df_col_list)
    left_list_miss_cols = case_insensitive_diff(
        right_df_col_list, left_df_col_list)
    return order_and_union_dataframes(left_df, right_df, list(left_list_miss_cols), list(right_list_miss_cols))


def rename_column(name: str) -> str:
    """
      Removes illegal column characters from a string

    :param name: String to format
    :return:
    """

    illegals = [
        ' ',
        ',',
        ';',
        '{',
        '}',
        '(',
        ')',
        '\t',
        '='
    ]

    for illegal in illegals:
        name = name.replace(illegal, '')

    return name


def rename_columns(dataframe: DataFrame) -> DataFrame:
    """
      Removes illegal characters from all columns

    :param dataframe: Source dataframe
    :return: Dataframe with renamed columns
    """
    return dataframe.select([col(c).alias(rename_column(c)) for c in dataframe.columns])


def copy_dataframe_to_socket(spark_session: SparkSession, src: JobSocket, dest: JobSocket,
                             read_options: Optional[dict] = None) -> None:
    """
      Copies data from src to dest JobSocket via a SparkSession

    :param spark_session: Spark Session to use for copying
    :param src: Source job socket
    :param dest: Destination job socket
    :param read_options: Spark session options to set when reading
    :return:
    """
    src_df = spark_session.read.format(src.data_format).options(**read_options).load(src.data_path)
    cleaned_columns_df = rename_columns(src_df)
    output_file_system = FileSystem.from_spark_session(spark_session)
    output_file_system.delete(path=dest.data_path, recursive=True)

    cleaned_columns_df.write.format(dest.data_format).save(dest.data_path)


def get_dataframe_columns(rows: Iterator[Row]) -> Iterator[HiveTableColumn]:
    """
     Reads columns from extended dataframe definition

    :param rows: Dataframe rows produced by describe table extended (df.toLocalIterator())
    :return:
    """
    for t_row in rows:
        if t_row['col_name']:
            yield HiveTableColumn(
                name=t_row['col_name'],
                type=t_row['data_type']
            )
        else:
            break


def get_dataframe_partitions(rows: Iterator[Row]) -> Iterator[Tuple[int, str]]:
    """
      Reads Partitioning section from extended table description
    :param rows: Dataframe rows produced by describe table extended (df.toLocalIterator())
    :return:
    """
    skip = True
    for t_row in rows:
        # catch when partitioning section starts
        if t_row["col_name"] == "# Partitioning":
            skip = False
            continue
        if not skip and t_row["col_name"]:
            yield int(str(t_row['col_name']).replace("Part ", "")), t_row['data_type']
        elif not skip and not t_row["col_name"]:
            break
