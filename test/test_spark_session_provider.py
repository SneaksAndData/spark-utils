import pytest
from spark_utils.common.spark_session_provider import SparkSessionProvider


def test_get_session():
    try:
        provider = SparkSessionProvider()
        spark_session = provider.get_session()
        spark_session.stop()
    except RuntimeError:
        pytest.fail("Failed to create and stop a Spark session")
