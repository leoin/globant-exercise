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

# COMMAND ----------

def insert_into_log(data,columns,log_table,status,error_msg):
    df_log = spark.createDataFrame(data,columns)
    df_log.write.mode("append").insertInto(log_table)
    df_log.display()

    if(status == 'ERROR'):
        assert False,error_msg

# COMMAND ----------

def get_last_record(df,window_spec):
    df_temp = df.withColumn(
        "row_number", 
        f.row_number().over(window_spec)
    ).where(
        f.col("row_number") == 1
    ).drop('row_number')

    return df_temp

# COMMAND ----------

def insert_into_qc_table(data,columns,qc_table):
    df_qc = spark.createDataFrame(data,columns)
    df_qc.write.mode("append").insertInto(qc_table)
    df_qc.display()
