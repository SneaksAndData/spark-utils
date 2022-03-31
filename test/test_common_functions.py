import os
import pytest
from typing import Tuple

from pyspark.sql import SparkSession

from spark_utils.models.job_socket import JobSocket
from spark_utils.common.functions import read_from_socket


@pytest.mark.parametrize('format_', [
    ('csv', {'header': True}),
    ('json', {}),
    ('parquet', {})
])
def test_read_from_socket(format_: Tuple[str, dict], spark_session: SparkSession, test_base_path: str):

    test_data_path = os.path.join(test_base_path, 'test_common_functions')

    socket = JobSocket(
        alias='test',
        data_path=f'file:///{os.path.join(test_data_path, f"data.{format_[0]}")}',
        data_format=format_[0],
    )
    df = read_from_socket(socket=socket, spark_session=spark_session, **format_[1])

    assert sorted(df.columns) == sorted(['strings', 'ints', 'floats'])


@pytest.mark.parametrize('sep', ['|', ';'])
def test_job_socket_serialize(sep: str, test_base_path: str):

    test_data_path = os.path.join(test_base_path, 'test_common_functions/data.parquet')
    socket = JobSocket(
        alias='test',
        data_path=os.path.join(test_data_path, 'data.parquet'),
        data_format='parquet',
    )

    assert socket.serialize(separator=sep) == f'{socket.alias}{sep}{socket.data_path}{sep}{socket.data_format}'


