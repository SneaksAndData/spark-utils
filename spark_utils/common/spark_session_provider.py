# MIT License
#
# Copyright (c) 2022 Ecco Sneaks & Data
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
  Provides convenient building of a Spark Session
"""
import os
import tempfile
from typing import Optional, List, Dict

from pyspark.sql import SparkSession

from spark_utils.models.hive_metastore_config import HiveMetastoreConfig


class SparkSessionProvider:
    """
      Provider of a Spark session and related objects
    """

    def __init__(self, *, delta_lake_version="2.12:2.1.0", hive_metastore_config: Optional[HiveMetastoreConfig] = None,
                 additional_packages: Optional[List[str]] = None,
                 additional_configs: Optional[Dict[str, str]] = None,
                 run_local=False):
        """
        :param delta_lake_version: Delta lake package version
        :param hive_metastore_config: Optional configuration of a hive metastore that should be connected to this Spark Session
        :param additional_packages: Additional jars to download. Would not override jars installed or provided from spark-submit.
         This setting only works if a session is started from python and not spark-submit
        :param additional_configs: Any additional spark configurations
        :param run_local: Whether single-node local master should be used.
        """

        packages = [f"io.delta:delta-core_{delta_lake_version}"]
        if additional_packages:
            packages.extend(additional_packages)

        self._spark_session_builder = SparkSession.builder \
            .config("spark.jars.packages", ",".join(packages)) \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.ivy", os.path.join(tempfile.gettempdir(), ".ivy2")) \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")

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

        if os.environ.get('PYTEST_CURRENT_TEST') or run_local:
            self._spark_session = self._spark_session_builder.master('local[*]').getOrCreate()
        else:
            self._spark_session = self._spark_session_builder.getOrCreate()

    def get_session(self):
        """
          Get a configured Spark Session

        :return: SparkSession
        """
        return self._spark_session
