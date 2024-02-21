"""
  Wrapper for DeltaLog class.
"""
from pyspark.sql import SparkSession
from hadoop_fs_wrapper.models.hadoop_fs_path import HadoopFsPath


# pylint: disable=W0212
class DeltaLog:
    """
    Wrapper for org.apache.spark.sql.delta.DeltaLog
    """

    def __init__(self, underlying):
        """
        Class init
        """
        self.underlying = underlying

    @classmethod
    def for_table(cls, spark_session: SparkSession, data_path: str) -> "DeltaLog":
        """
         Wraps constructor: def forTable(spark: SparkSession, dataPath: String): DeltaLog

         Helper for creating a log when it stored at the root of the data

        :param data_path: Path to delta table.
        :return: DeltaLog.
        """
        return cls(
            spark_session._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark_session._jsparkSession, data_path)
        )

    @staticmethod
    def invalidate_cache(spark_session: SparkSession, data_path: str) -> None:
        """
        Invalidate the cached DeltaLog object for the given data_path.
        """

        hadoop_path = HadoopFsPath.from_string(spark_session._jvm, data_path)

        spark_session._jvm.org.apache.spark.sql.delta.DeltaLog.invalidateCache(
            spark_session._jsparkSession, hadoop_path.underlying
        )

    @staticmethod
    def clear_cache(spark_session: SparkSession):
        """
        Clears all delta log caches associated with this Spark session.
        """
        spark_session._jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()

    def table_id(self) -> str:
        """
        The unique identifier for this table.
        """
        return self.underlying.tableId()
