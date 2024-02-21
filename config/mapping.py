# Databricks notebook source
# MAGIC %run ./functions

# COMMAND ----------

custom_schema = {
    'jobs':StructType([
        StructField("id", StringType(), True),
        StructField("jobs", StringType(), True)
    ]),
    'departments':StructType([
        StructField("id", StringType(), True),
        StructField("department", StringType(), True)
    ]),
    'hired_employees':StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("datetime", StringType(), True),
        StructField("department_id", StringType(), True),
        StructField("job_id", StringType(), True)
    ])
}

# COMMAND ----------

ids_mapping = {
    'jobs':['id'],
    'departments':['id'],
    'hired_employees':['id']
}
