"""Criação da SparkSession local usada por todas as etapas."""
import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "ecommerce-pipeline") -> SparkSession:
    # Garante que o Spark use o mesmo Python do ambiente virtual (evita erro no Windows)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
