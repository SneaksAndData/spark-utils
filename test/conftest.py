import pytest
import pathlib

from spark_utils.common.spark_session_provider import SparkSessionProvider


@pytest.fixture(scope="module")
def spark_session():
    java_17_launch_options = "-XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED --add-opens=java.base/java.util.stream=ALL-UNNAMED"
    return SparkSessionProvider(
        additional_configs={
            'spark.driver.extraJavaOptions': java_17_launch_options,
            'spark.executor.extraJavaOptions': java_17_launch_options
        }
    ).get_session()


@pytest.fixture(scope="module")
def test_base_path():
    return f"{pathlib.Path(__file__).parent.resolve()}"

