from delta import DeltaTable
from pyspark.sql import SparkSession

from hadoop_fs_wrapper.wrappers.file_system import FileSystem

from src.spark_utils.common.spark_job_args import SparkJobArgs
from src.spark_utils.models.job_socket import JobSocket


def merge_or_create_table(spark_args: SparkJobArgs, spark_session: SparkSession, table_name: str,
                          output_socket: JobSocket,
                          merge_key_column="mergeKey"):
    output_file_system = FileSystem.from_spark_session(spark_session)

    if spark_args.overwrite():
        output_file_system.delete(path=output_socket.data_path, recursive=True)
        spark_session.table(table_name).write.format(output_socket.data_format).mode('overwrite').save(
            output_socket.data_path)
    else:
        delta_table = DeltaTable.forPath(spark_session, output_socket.data_path)
        delta_table.alias("target").merge(
            spark_session.table(table_name).alias("stagingData"),
            f"""
            stagingData.{merge_key_column} <=> target.{merge_key_column}
            """) \
            .whenNotMatchedInsertAll() \
            .execute()
