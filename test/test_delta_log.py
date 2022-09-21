import time

from pyspark.sql import SparkSession

from spark_utils.delta_lake.delta_log import DeltaLog


def test_table_id(spark_session: SparkSession, test_base_path: str):
    delta_log = DeltaLog.for_table(spark_session, f'file:///{test_base_path}/delta_log/table1')

    assert delta_log.table_id() == 'f5606f77-d2b4-4ca6-a676-8cd761aefdc2'


def test_cache_clear(spark_session: SparkSession, test_base_path: str):
    # Guava cache itself is private, so we evaluate cache vs no-cache using read times
    start = time.monotonic_ns()
    _ = DeltaLog.for_table(spark_session, f'file:///{test_base_path}/delta_log/table2')
    non_cached_read = time.monotonic_ns() - start

    start = time.monotonic_ns()
    _ = DeltaLog.for_table(spark_session, f'file:///{test_base_path}/delta_log/table2')
    cached_read = time.monotonic_ns() - start

    DeltaLog.clear_cache(spark_session)

    start = time.monotonic_ns()
    _ = DeltaLog.for_table(spark_session, f'file:///{test_base_path}/delta_log/table2')
    cleared_cache_read = time.monotonic_ns() - start

    assert non_cached_read / cached_read > 100 and non_cached_read / cleared_cache_read < 2
