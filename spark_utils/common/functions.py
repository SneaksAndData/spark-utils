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
  Common helper functions.
"""
import os
from typing import Optional, List, Dict

from cryptography.fernet import Fernet
from pyspark.sql import SparkSession, DataFrame
from hadoop_fs_wrapper.wrappers.file_system import FileSystem

from spark_utils.models.job_socket import JobSocket


def is_valid_source_path(file_system: FileSystem, path: str):
    """
     Checks whether a regexp path refers to a valid set of paths
    :param file_system: pyHadooopWrapper FileSystem
    :param path: path e.g. abfss://hello@world.com/path/part*.csv
    :return: dict containing "base_path" and "glob_filter"
    """
    return len(file_system.glob_status(path)) > 0


def decrypt_sensitive(sensitive_content: Optional[str]) -> Optional[str]:
    """
      Decrypts a provided string

    :param sensitive_content: payload to decrypt
    :return: Decrypted payload
    """
    encryption_key = os.environ.get("RUNTIME_ENCRYPTION_KEY", "").encode("utf-8")

    if not encryption_key:
        print("Encryption key not set - skipping operation.")

    if encryption_key and sensitive_content:
        fernet = Fernet(encryption_key)
        return fernet.decrypt(sensitive_content.encode("utf-8")).decode("utf-8")

    return None


def read_from_socket(
    socket: JobSocket,
    spark_session: SparkSession,
    read_options: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Reads data with location specified by socket. kwargs are passed to spark options

    :param socket: Socket
    :param spark_session: Spark session
    :param read_options: Read options passed to spark (e.g. Parquet options
    found here: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option)
    :return: Spark dataframe
    """
    read_options = read_options or {}
    if socket.data_format.startswith("hive"):
        return spark_session.table(socket.data_path)

    return spark_session.read.options(**read_options).format(socket.data_format).load(socket.data_path)


def write_to_socket(
    data: DataFrame,
    socket: JobSocket,
    partition_by: Optional[List[str]] = None,
    partition_count: Optional[int] = None,
    write_options: Optional[Dict[str, str]] = None,
) -> None:
    """Writes data to socket

    :param data: Dataframe to write
    :param socket: Socket to write to
    :param partition_by: List of column names to partition by
    :param partition_count: Number of partitions to split result into.
    :param write_options: Write options passed to spark (e.g. Parquet options
    found here: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#data-source-option)
    """
    write_options = write_options or {}
    partition_by = partition_by or []
    if partition_count:
        data = data.repartition(partition_count, *partition_by)

    writer = data.write.mode("overwrite").options(**write_options)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if socket.data_format.startswith("hive"):
        writer.format(socket.data_format.split("_")[-1]).saveAsTable(socket.data_path)
    else:
        writer.format(socket.data_format).save(socket.data_path)
