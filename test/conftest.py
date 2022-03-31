import pytest
import pathlib

from spark_utils.common.spark_session_provider import SparkSessionProvider


@pytest.fixture(scope="module")
def spark_session():
    provider = SparkSessionProvider()
    return provider.get_session()


@pytest.fixture(scope="module")
def test_base_path():
    return f"{pathlib.Path(__file__).parent.resolve()}"

