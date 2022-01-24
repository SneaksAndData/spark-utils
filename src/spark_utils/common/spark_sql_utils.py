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

"""Utility methods for Spark SQL"""
from typing import List

from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField

from hadoop_fs_wrapper.wrappers.file_system import FileSystem

from spark_utils.common.spark_job_args import SparkJobArgs
from spark_utils.models.job_socket import JobSocket
from spark_utils.dataframes.functions import union_dataframes


def merge_or_create_table(spark_args: SparkJobArgs, spark_session: SparkSession, table_name: str,
                          output_socket: JobSocket,
                          merge_key_column="mergeKey"):
    """
     Writes a specified SQL table or view to the provided output

    :param spark_args: Parent job runtime arguments
    :param spark_session: Spark Session to use when writing
    :param table_name: Table to write
    :param output_socket: JobSocket definition for the output
    :param merge_key_column: Table column to use when merging data
    :return:
    """
    output_file_system = FileSystem.from_spark_session(spark_session)

    if spark_args.overwrite():
        output_file_system.delete(path=output_socket.data_path, recursive=True)
        spark_session.table(table_name).write.format(output_socket.data_format).mode('overwrite').save(
            output_socket.data_path)
    else:
        delta_table = DeltaTable.forPath(spark_session, output_socket.data_path)
        delta_table.alias("target").merge(
            spark_session.table(table_name).alias("stagingData"),
            f"""
            stagingData.{merge_key_column} <=> target.{merge_key_column}
            """) \
            .whenNotMatchedInsertAll() \
            .execute()


def union_views_to_schema(spark_session: SparkSession, schema: List[StructField], views: List[str]) -> DataFrame:
    """
      Combines all views and forces them into provided schema by adding missing columns filled with nulls
    :param spark_session: Spark Session to run the statement
    :param schema: Schema to enforce
    :param views: List of views to combine
    :return: Combined dataframe
    """
    df_schema = spark_session.createDataFrame([], StructType(schema))
    for view in views:
        df_schema = union_dataframes(df_schema, spark_session.table(view))
    return df_schema
