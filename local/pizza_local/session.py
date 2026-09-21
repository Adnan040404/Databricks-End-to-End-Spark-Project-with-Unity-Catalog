"""A small local Spark session, so the project runs without Databricks."""

import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name="pizza-analytics", cores=2):
    # Use the same Python for the driver and the workers, or Spark on Windows can pick another one.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (SparkSession.builder.master(f"local[{cores}]").appName(app_name)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")      # small data: 200 partitions is wasteful
             .config("spark.ui.enabled", "false")
             .config("spark.driver.host", "127.0.0.1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark
