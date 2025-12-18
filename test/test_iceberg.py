import pytest
from pyspark.sql import SparkSession, DataFrame


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
