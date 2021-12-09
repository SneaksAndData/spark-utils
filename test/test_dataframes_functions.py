import os
import pathlib
import pytest
from hadoop_fs_wrapper.wrappers.file_system import FileSystem

from pyspark.sql import SparkSession

from datetime import datetime

from spark_utils.common.functions import is_valid_source_path
from spark_utils.dataframes.functions import copy_dataframe_to_socket
from spark_utils.common.spark_session_provider import SparkSessionProvider
from spark_utils.dataframes.models import CopyDataOptions
from spark_utils.models.job_socket import JobSocket


@pytest.fixture()
def spark_session():
    provider = SparkSessionProvider()
    return provider.get_session()


def test_copy_dataframe_to_socket(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/copy_dataframe_to_socket"

    copy_dataframe_to_socket(
        spark_session=spark_session,
        copy_options=CopyDataOptions(
            src=JobSocket('src', f'file:///{test_data_path}/file-to-copy', 'csv'),
            dest=JobSocket('dst', f'file:///{test_data_path}/out-with-copy', 'json'),
            read_options={
                "delimiter": ";",
                "header": "true"
            }
        )
    )

    files = os.listdir(f"{test_data_path}/out-with-copy")
    assert is_valid_source_path(FileSystem.from_spark_session(spark_session), path=f"{test_data_path}/out-with-copy") and len(
        files) > 0


def test_copy_dataframe_to_socket_with_filename(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/copy_dataframe_to_socket"

    copy_dataframe_to_socket(
        spark_session=spark_session,
        copy_options=CopyDataOptions(
            src=JobSocket('src', f'file:///{test_data_path}/file-to-copy', 'csv'),
            dest=JobSocket('dst', f'file:///{test_data_path}/out-with-filename', 'json'),
            read_options={
                "delimiter": ";",
                "header": "true"
            },
            include_filename=True
        )
    )

    files = [file for file in os.listdir(f"{test_data_path}/out-with-filename") if file.endswith(".json")]
    file_contents = open(f"{test_data_path}/out-with-filename/{files[0]}", 'r').read()
    assert 'file-to-copy' in file_contents


def test_copy_dataframe_to_socket_with_sequence(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/copy_dataframe_to_socket"

    copy_dataframe_to_socket(
        spark_session=spark_session,
        copy_options=CopyDataOptions(
            src=JobSocket('src', f'file:///{test_data_path}/file-to-copy', 'csv'),
            dest=JobSocket('dst', f'file:///{test_data_path}/out-with-row-sequence', 'json'),
            read_options={
                "delimiter": ";",
                "header": "true"
            },
            include_filename=True,
            include_row_sequence=True
        )
    )

    files = [file for file in os.listdir(f"{test_data_path}/out-with-row-sequence") if file.endswith(".json")]
    file_contents = open(f"{test_data_path}/out-with-row-sequence/{files[0]}", 'r').read()
    assert 'row_sequence' in file_contents
    assert '0' in file_contents


def test_copy_dataframe_to_socket_with_timestamp(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/copy_dataframe_to_socket"

    copy_stats = copy_dataframe_to_socket(
        spark_session=spark_session,
        copy_options=CopyDataOptions(
            src=JobSocket('src', f'file:///{test_data_path}/file-to-copy-with-ts', 'csv'),
            dest=JobSocket('dst', f'file:///{test_data_path}/out-with-ts', 'json'),
            read_options={
                "delimiter": ";",
                "header": "true"
            },
            include_filename=True,
            timestamp_column='ts',
            timestamp_column_format="yyyy-MM-dd'T'HH:mm:ss"
        )
    )

    approx_age = (datetime.now() - datetime(2021, 10, 6, 1, 0, 0)).total_seconds()

    assert copy_stats['original_row_count'] == 0 \
           and copy_stats['original_content_age'] == 0 \
           and copy_stats['row_count'] == 4 \
           and 1 - copy_stats['content_age'] / approx_age < 0.01
