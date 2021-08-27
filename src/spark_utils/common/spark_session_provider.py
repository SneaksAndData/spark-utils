"""
  Provides convenient building of a Spark Session
"""
import os
from typing import Optional, List, Dict

from pyspark.sql import SparkSession

from spark_utils.models.hive_metastore_config import HiveMetastoreConfig


class SparkSessionProvider:
    """
      Provider of a Spark session and related objects
    """

    def __init__(self, *, delta_lake_version="2.12:1.0.0", hive_metastore_config: Optional[HiveMetastoreConfig] = None,
                 additional_packages: Optional[List[str]] = None,
                 additional_configs: Optional[Dict[str, str]] = None):
        """
        :param delta_lake_version: Delta lake package version
        :param hive_metastore_uri: Optional URI of a hive metastore that should be connected to this Spark Session
        """

        packages = [f"io.delta:delta-core_{delta_lake_version}"]
        if additional_packages:
            packages.extend(additional_packages)

        self._spark_session_builder = SparkSession.builder \
            .config("spark.jars.packages", ",".join(packages)) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.ivy", "/tmp/.ivy2") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.driver.extraJavaOptions",
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError='kill -9 %p'") \
            .config("spark.executor.extraJavaOptions",
                    "-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError='kill -9 %p'")

        if hive_metastore_config:
            if hive_metastore_config.connection_driver_name:
                self._spark_session_builder = self._spark_session_builder \
                    .config("spark.hadoop.javax.jdo.option.ConnectionURL", hive_metastore_config.connection_url) \
                    .config("spark.hadoop.javax.jdo.option.ConnectionUserName",
                            hive_metastore_config.connection_username) \
                    .config("spark.hadoop.javax.jdo.option.ConnectionPassword",
                            hive_metastore_config.connection_password) \
                    .config("spark.hadoop.javax.jdo.option.ConnectionDriverName",
                            hive_metastore_config.connection_driver_name)
            elif hive_metastore_config.metastore_uri:
                self._spark_session_builder = self._spark_session_builder \
                    .config("spark.hadoop.hive.metastore.uris", hive_metastore_config.metastore_uri)
            else:
                raise ValueError("Invalid Hive Metastore Configuration provided")

            self._spark_session_builder = self._spark_session_builder \
                .config("spark.sql.hive.metastore.version", hive_metastore_config.metastore_version) \
                .config("spark.sql.hive.metastore.jars", hive_metastore_config.metastore_jars) \
                .config("spark.sql.catalogImplementation", "hive")

        if additional_configs:
            for config_key, config_value in additional_configs.items():
                self._spark_session_builder = self._spark_session_builder.config(config_key, config_value)

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
