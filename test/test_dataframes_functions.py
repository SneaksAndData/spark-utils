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
