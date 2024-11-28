from spark_utils.delta_lake.functions import delta_compact_v2
from glob import glob
from pyspark.sql import SparkSession

from tests.common import generate_table


def test_compact_v2(spark_session: SparkSession, semantic_logger):
    test_data_path = generate_table(spark_session, "compact_v2")

    delta_compact_v2(
        spark_session=spark_session,
        path=f"file://{test_data_path}",
        logger=semantic_logger,
        retain_hours=0,
        vacuum_only=False,
    )

    num_parquet_files = len(glob(f"{test_data_path}/*.parquet"))
    num_log_files = len(glob(f"{test_data_path}/_delta_log/*.json"))

    # logs are cleaned on a daily basis, so we cannot test the log retention
    assert num_parquet_files == 1 and num_log_files == 14
