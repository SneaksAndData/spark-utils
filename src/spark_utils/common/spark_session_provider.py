"""
  Provides convenient building of a Spark Session
"""
import os

from pyspark.sql import SparkSession


class SparkSessionProvider:
    """
      Provider of a Spark session and related objects
    """

    def __init__(self, *, delta_lake_version="2.12:1.0.0"):
        """
        :param delta_lake_version: Delta lake package version
        """
        self._spark_session_builder = SparkSession.builder \
            .config("spark.jars.packages", f"io.delta:delta-core_{delta_lake_version}") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .config("spark.driver.extraJavaOptions",
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError='kill -9 %p'") \
            .config("spark.executor.extraJavaOptions",
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError='kill -9 %p'")

        if os.environ.get('PYTEST_CURRENT_TEST'):
            self._spark_session = self._spark_session_builder.master('local[*]').getOrCreate()
        else:
            self._spark_session = self._spark_session_builder.getOrCreate()

    def get_session(self):
        """
          Get a configured Spark Session

        :return: SparkSession
        """
        return self._spark_session
