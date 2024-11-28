import pathlib

from pyspark.sql import SparkSession


def generate_table(spark_session: SparkSession, suffix: str, dir=None) -> str:
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/{suffix}" if not dir else dir
    df = spark_session.range(100)

    for _ in range(10):
        df.write.format("delta").mode("overwrite").save(test_data_path)

    return test_data_path
