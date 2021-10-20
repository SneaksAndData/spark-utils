import os
import pathlib
import pytest

from pyspark.sql import SparkSession

from spark_utils.dataframes.functions import copy_dataframe_to_socket
from spark_utils.common.spark_session_provider import SparkSessionProvider
from spark_utils.models.job_socket import JobSocket


@pytest.fixture()
def spark_session():
    provider = SparkSessionProvider()
    return provider.get_session()


def test_copy_dataframe_to_socket(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/copy_dataframe_to_socket"

    copy_dataframe_to_socket(
        spark_session=spark_session,
        src=JobSocket('src', f'file:///{test_data_path}/file-to-copy', 'csv'),
        dest=JobSocket('dst', f'file:///{test_data_path}/out', 'json'),
        read_options={
            "delimiter": ";",
            "header": "true"
        }
    )

    files = os.listdir(f"{test_data_path}/out")
    assert len(files) > 0


def test_copy_dataframe_to_socket_with_filename(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/copy_dataframe_to_socket"

    copy_dataframe_to_socket(
        spark_session=spark_session,
        src=JobSocket('src', f'file:///{test_data_path}/file-to-copy', 'csv'),
        dest=JobSocket('dst', f'file:///{test_data_path}/out', 'json'),
        read_options={
            "delimiter": ";",
            "header": "true"
        },
        include_filename=True
    )

    files = [file for file in os.listdir(f"{test_data_path}/out") if file.endswith(".json")]
    file_contents = open(f"{test_data_path}/out/{files[0]}", 'r').read()
    assert 'file-to-copy' in file_contents

def test_copy_dataframe_to_socket_with_sequence(spark_session: SparkSession):
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/copy_dataframe_to_socket"

    copy_dataframe_to_socket(
        spark_session=spark_session,
        src=JobSocket('src', f'file:///{test_data_path}/file-to-copy', 'csv'),
        dest=JobSocket('dst', f'file:///{test_data_path}/out', 'json'),
        read_options={
            "delimiter": ";",
            "header": "true"
        },
        include_filename=True,
        include_row_sequence=True
    )

    files = [file for file in os.listdir(f"{test_data_path}/out") if file.endswith(".json")]
    file_contents = open(f"{test_data_path}/out/{files[0]}", 'r').read()
    assert 'row_sequence' in file_contents
    assert '0' in file_contents
