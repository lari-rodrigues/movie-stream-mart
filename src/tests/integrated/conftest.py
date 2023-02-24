import pytest
from pyspark.sql import SparkSession
import os
import shutil

from src.db_migration.ddl import do_migration


@pytest.fixture(scope="session")
def spark_session():
    builder = (SparkSession.builder.appName("MyApp")
                .enableHiveSupport()
                .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.2")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.sql.warehouse.dir", "warehouse")
                .enableHiveSupport())
    
    return builder.getOrCreate()


def remove_dir(dirpath):
    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        shutil.rmtree(dirpath)

def pytest_configure(config):
    """
    This function runs before each integrated test. This ensures that the tests are isolated and one does not impact the other
    """
    remove_dir("warehouse")
    remove_dir("metastore_db")
    do_migration()