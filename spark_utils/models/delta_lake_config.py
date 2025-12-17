from dataclasses import dataclass


@dataclass
class DeltaLakeConfig:
    version: str = "io.delta:delta-spark_2.12:3.2.1"
    catalog_extension: str = "io.delta.sql.DeltaSparkSessionExtension"
    spark_catalog_class: str = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
