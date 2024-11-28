from spark_utils.delta_lake.functions import delta_compact
from glob import glob
from pyspark.sql import SparkSession

from test.common import generate_table


def test_delta_compact(spark_session: SparkSession):
    test_data_path = generate_table(spark_session, "compact")

    delta_compact(
        spark_session=spark_session,
        path=f"file://{test_data_path}",
        retain_hours=0,
        vacuum_only=False,
    )

    num_parquet_files = len(glob(f"{test_data_path}/*.parquet"))
    num_log_files = len(glob(f"{test_data_path}/_delta_log/*.json"))

    # logs are cleaned on a daily basis, so we cannot test the log retention
    assert num_parquet_files == 1 and num_log_files == 4
