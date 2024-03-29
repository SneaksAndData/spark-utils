import os
from typing import List, Union

import pytest
from collections import namedtuple

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from spark_utils.dataframes.functions import rename_column
from spark_utils.models.job_socket import JobSocket
from spark_utils.common.functions import read_from_socket, write_to_socket


def are_dfs_equal(df1: DataFrame, df2: DataFrame) -> bool:
    """Asserts if dataframes are equal"""
    sort_col = df1.columns[0]
    return (df1.schema == df2.schema) and (df1.sort(sort_col).collect() == df2.sort(sort_col).collect())


Format = namedtuple("format", ["format", "read_options"])


@pytest.mark.parametrize(
    "format_",
    [
        Format(format="csv", read_options={"header": True}),
        Format(format="json", read_options={}),
        Format(format="parquet", read_options={}),
    ],
)
def test_read_from_socket(format_: Format, spark_session: SparkSession, test_base_path: str):
    test_data_path = os.path.join(test_base_path, "test_common_functions")

    socket = JobSocket(
        alias="test",
        data_path=f'file:///{os.path.join(test_data_path, f"data.{format_.format}")}',
        data_format=format_.format,
    )
    df = read_from_socket(socket=socket, spark_session=spark_session, read_options=format_.read_options)

    assert sorted(df.columns) == sorted(["strings", "ints", "floats"])


Format = namedtuple("format", ["format", "read_options"])


@pytest.mark.parametrize(
    "format_",
    [
        Format(format="parquet", read_options={}),
        Format(format="csv", read_options={"header": True}),
        Format(format="json", read_options={}),
    ],
)
@pytest.mark.parametrize("partition_by", [["strings"], None, []])
@pytest.mark.parametrize("partition_count", [None, 1, 2])
def test_write_to_socket(
    format_: Format,
    spark_session: SparkSession,
    test_base_path: str,
    partition_by: Union[None, List[str]],
    partition_count: Union[None, int],
):
    test_data_path = os.path.join(test_base_path, "test_common_functions")
    socket = JobSocket(
        alias="test",
        data_path=f'file:///{os.path.join(test_data_path, f"data.{format_.format}")}',
        data_format=format_.format,
    )
    output_socket = JobSocket(
        alias="test",
        data_path=f'file:///{os.path.join(test_data_path, "write", f"data.{format_.format}")}',
        data_format=format_.format,
    )
    df = read_from_socket(socket=socket, spark_session=spark_session, read_options=format_.read_options)

    write_to_socket(
        data=df,
        socket=output_socket,
        write_options=format_.read_options,
        partition_by=partition_by,
        partition_count=partition_count,
    )

    df_read = read_from_socket(socket=output_socket, spark_session=spark_session, read_options=format_.read_options)

    assert are_dfs_equal(df, df_read.select(df.columns))


@pytest.mark.parametrize("sep", ["|", ";"])
def test_job_socket_serialize(sep: str, test_base_path: str):
    test_data_path = os.path.join(test_base_path, "test_common_functions/data.parquet")
    socket = JobSocket(
        alias="test",
        data_path=os.path.join(test_data_path, "data.parquet"),
        data_format="parquet",
    )

    assert socket.serialize(separator=sep) == f"{socket.alias}{sep}{socket.data_path}{sep}{socket.data_format}"


@pytest.mark.parametrize(
    "funky_name, expected_name",
    [
        ("a--bc", "abc"),
        (".abc", "abc"),
        ("a bc", "abc"),
        ("a\\bc", "abc"),
        ("a/bc", "abc"),
        ("a\t{};,bc", "abc"),
    ],
)
def test_column_rename(funky_name: str, expected_name: str):
    assert expected_name == rename_column(funky_name)
