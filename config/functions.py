# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as f
from pyspark.sql.window import Window

# COMMAND ----------

def return_actual_timestamp():
    from datetime import datetime

    # Get the current timestamp
    current_timestamp = datetime.now()
    timestamp_str = current_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    return timestamp_str
