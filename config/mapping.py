# Databricks notebook source
# MAGIC %run ./functions

# COMMAND ----------

# This mapping is to pass the structure per table in bronze in order to make 1 notebook

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

# This mapping is to pass the list of ids in silver notebook to do the merge

ids_mapping = {
    'jobs':['id'],
    'departments':['id'],
    'hired_employees':['id']
}
