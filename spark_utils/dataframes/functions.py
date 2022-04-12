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
  Helper functions for Spark Dataframes
"""

from typing import List, Iterator, Tuple

from datetime import datetime

from hadoop_fs_wrapper.wrappers.file_system import FileSystem
from pyspark import Row
from pyspark.sql import DataFrame, Column, SparkSession
from pyspark.sql.functions import lit, col, input_file_name, to_timestamp, max as df_max

from spark_utils.common.functions import is_valid_source_path
from spark_utils.dataframes.models import CopyDataOptions
from spark_utils.dataframes.sets.functions import case_insensitive_diff
from spark_utils.models.hive_table import HiveTableColumn


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
    for missing_col in missing_column_names:
        source_df = source_df.withColumn(missing_col, empty_column(missing_col))
    return source_df


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
        '=',
        '/',
        '\\',
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


def _max_timestamp(dataframe: DataFrame, timestamp_column: str, timestamp_column_format: str) -> datetime:
    """
      Returns max of a provided timestamp column, using provided format.
    :param dataframe: Dataframe to perform computation on.
    :param timestamp_column: Timestamp column to max.
    :param timestamp_column_format: Timestamp column format.
    :return: max timestamp.
    """
    return dataframe.select(
        to_timestamp(col(timestamp_column), timestamp_column_format).alias(timestamp_column)).agg(
        df_max(col(timestamp_column)).alias(timestamp_column)).head(1)[0][0]


def copy_dataframe_to_socket(spark_session: SparkSession,
                             copy_options: CopyDataOptions) -> dict:
    """
      Copies data from src to dest JobSocket via a SparkSession

    :param spark_session: Spark Session to use for copying
    :param copy_options: Copy options.
    :return: dict of (original_row_count, original_content_age, row_count, content_age)
    """

    print(f"Running data copy using options: {copy_options}")

    copy_options.read_options = copy_options.read_options or {}
    copy_options.write_options = copy_options.write_options or {}

    if copy_options.timestamp_column:
        assert copy_options.timestamp_column_format, \
            'When specifying timestamp_column, you must provide timestamp_column_format as well.'

    output_file_system = FileSystem.from_spark_session(spark_session)

    if copy_options.clean_destination:
        output_file_system.delete(path=copy_options.dest.data_path, recursive=True)

    copy_stats = {
        'original_row_count': 0,
        'original_content_age': 0,
        'row_count': 0,
        'content_age': 0
    }

    if is_valid_source_path(
            file_system=output_file_system,
            path=copy_options.dest.data_path
    ):
        original_df = spark_session.read.format(copy_options.dest.data_format).load(copy_options.dest.data_path)
        copy_stats['original_row_count'] = original_df.count()
        if copy_options.timestamp_column:
            original_max_ts = _max_timestamp(original_df, copy_options.timestamp_column,
                                             copy_options.timestamp_column_format)
            copy_stats['original_content_age'] = int((datetime.utcnow() - original_max_ts).total_seconds())

    source_df = spark_session.read.format(copy_options.src.data_format).options(**copy_options.read_options).load(
        copy_options.src.data_path)

    if copy_options.clean_column_names:
        source_df = rename_columns(source_df)

    if copy_options.include_filename:
        source_df = source_df \
            .withColumn("filename", input_file_name())

    if copy_options.include_row_sequence:
        source_df = source_df \
            .rdd.zipWithIndex() \
            .map(lambda x: list(x[0]) + [x[1]]) \
            .toDF(source_df.withColumn('row_sequence', lit(0)).schema)

    copy_stats['row_count'] = source_df.count()

    if copy_options.timestamp_column:
        max_ts = _max_timestamp(source_df, copy_options.timestamp_column, copy_options.timestamp_column_format)
        copy_stats['content_age'] = int((datetime.utcnow() - max_ts).total_seconds())

    if copy_options.output_file_count:
        source_df = source_df.repartition(copy_options.output_file_count)

    source_df \
        .write \
        .format(copy_options.dest.data_format) \
        .options(**copy_options.write_options) \
        .save(path=copy_options.dest.data_path, mode='errorifexists' if copy_options.clean_destination else 'append')

    return copy_stats


def get_dataframe_columns(rows: Iterator[Row]) -> Iterator[HiveTableColumn]:
    """
     Reads columns from extended dataframe definition

    :param rows: Dataframe rows produced by describe table extended (df.toLocalIterator())
    :return:
    """
    for t_row in rows:
        if t_row['col_name']:
            yield HiveTableColumn(
                name=rename_column(t_row['col_name']),
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
            part_index_str = str(t_row['col_name']).replace("Part ", "")
            if part_index_str.isdigit():
                yield int(part_index_str), rename_column(t_row['data_type'])
            else:
                break
        elif not skip and not t_row["col_name"]:
            break
