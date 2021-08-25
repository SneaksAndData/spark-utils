import pytest
from spark_utils.common.spark_session_provider import SparkSessionProvider
from spark_utils.models.hive_metastore_config import HiveMetastoreConfig


def test_get_session():
    try:
        provider = SparkSessionProvider()
        spark_session = provider.get_session()
        spark_session.stop()
    except RuntimeError:
        pytest.fail("Failed to create and stop a Spark session")


def test_get_session_with_hive():
    try:
        provider = SparkSessionProvider(
            hive_metastore_config=HiveMetastoreConfig(
                metastore_version="3.1.2",
                metastore_jars="maven",
                metastore_uri="thrift://test:9083"
            ),
        )
        spark_session = provider.get_session()
        spark_session.stop()
    except RuntimeError:
        pytest.fail("Failed to create and stop a Spark session with metastore jars from maven")


def test_get_session_with_packages():
    try:
        provider = SparkSessionProvider(
            additional_packages=["spark-bigquery_2.12:0.22.0"],
        )
        spark_session = provider.get_session()
        spark_session.stop()
    except RuntimeError:
        pytest.fail("Failed to create and stop a Spark session with additional packages from maven")