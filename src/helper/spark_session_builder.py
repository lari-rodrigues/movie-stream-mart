from os.path import abspath

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging
import os


class SparkSessionBuilder:
    def __init__(self, warehouse_location: str = None):
        if not warehouse_location:
            warehouse_location = os.path.join(os.getcwd(), 'warehouse')
        self._warehouse_location = warehouse_location

    def build(self) -> SparkSession:
        logging.info("Building SparkSession")

        builder = (SparkSession.builder.appName("MyApp")
                   .enableHiveSupport()
                   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                   .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                   .config("spark.sql.warehouse.dir", self._warehouse_location)
                   .enableHiveSupport())

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        return spark
