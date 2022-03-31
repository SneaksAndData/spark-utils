import os
import pytest
from collections import namedtuple

from pyspark.sql import SparkSession

from spark_utils.models.job_socket import JobSocket
from spark_utils.common.functions import read_from_socket

Format = namedtuple('format', ['format', 'spark_options'])
@pytest.mark.parametrize('format_', [
    Format(format='csv', spark_options={'header': True}),
    Format(format='json', spark_options={}),
    Format(format='parquet', spark_options={}),
])
def test_read_from_socket(format_: Format, spark_session: SparkSession, test_base_path: str):

    test_data_path = os.path.join(test_base_path, 'test_common_functions')

    socket = JobSocket(
        alias='test',
        data_path=f'file:///{os.path.join(test_data_path, f"data.{format_.format}")}',
        data_format=format_.format,
    )
    df = read_from_socket(socket=socket, spark_session=spark_session, **format_.spark_options)

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


