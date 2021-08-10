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
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

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

    def get_file_system(self, path) -> HadoopFsWrapper:
        """
          Get a Hadoop FileSystem used by current Spark Session
        :return: HadoopFsWrapper
        """
        spark_jvm = self._spark_session._jvm
        init_path = spark_jvm.java.net.URI(path)
        if os.environ.get('PYTEST_CURRENT_TEST'):
            return HadoopFsWrapper(
                obj=spark_jvm.org.apache.hadoop.fs.FileSystem.get(
                    init_path,
                    self._spark_session._jsc.hadoopConfiguration()
                ),
                jvm=spark_jvm
            )

        return HadoopFsWrapper(
            obj=spark_jvm.org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem.get(
                init_path,
                self._spark_session._jsc.hadoopConfiguration()
            ),
            jvm=spark_jvm
        )
