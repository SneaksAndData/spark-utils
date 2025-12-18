from pyspark.sql import SparkSession, DataFrame

from spark_utils.common.functions import write_to_socket, read_from_socket
from spark_utils.models.job_socket import JobSocket
from test.test_common_functions import are_dfs_equal


def test_iceberg_rest_create_schema(iceberg_spark_session: SparkSession):
    try:
        s: DataFrame = iceberg_spark_session.sql("CREATE SCHEMA IF NOT EXISTS iceberg.test_schema")
        print(s.collect())
    except BaseException as e:
        raise RuntimeError("Failed to create schema") from e


def test_iceberg_rest_create_table(iceberg_spark_session: SparkSession):
    try:
        _ = iceberg_spark_session.sql(
            "CREATE OR REPLACE TABLE iceberg.test.simple_table AS SELECT 1 AS C0, '1231' AS C1, 1.0 AS C2"
        )
        rows = iceberg_spark_session.sql("SELECT * FROM iceberg.test.simple_table")
        assert rows.collect()[0].asDict() == {"C0": 1, "C1": "1231", "C2": 1.0}
    except BaseException as e:
        raise RuntimeError("Failed to create table") from e


def test_write_to_socket(
    iceberg_spark_session: SparkSession,
):
    output_socket = JobSocket(
        alias="test",
        data_path=f"iceberg.test.job_socket_write",
        data_format="iceberg",
    )
    df = iceberg_spark_session.createDataFrame(
        [{"C0": 1, "C1": "1231", "C2": 1.0}, {"C0": 2, "C1": "1232", "C2": 2.0}, {"C0": 3, "C1": "1233", "C2": 3.0}]
    )

    write_to_socket(
        data=df,
        socket=output_socket,
        write_options=None,
        partition_by=None,
        partition_count=None,
    )

    df_read = read_from_socket(socket=output_socket, spark_session=iceberg_spark_session, read_options=None)

    assert are_dfs_equal(df, df_read.select(df.columns))
