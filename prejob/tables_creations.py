# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("hive_schema", "")

hive_schema = dbutils.widgets.get("hive_schema")

print(f'''
hive_schema={hive_schema}
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables

# COMMAND ----------

table_name = 'jobs'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.bronze.{table_name} (
        id STRING, 
        jobs STRING
    )
    USING DELTA
    PARTITIONED BY (process_date TIMESTAMP)
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)

# COMMAND ----------

table_name = 'departments'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.bronze.{table_name} (
        id STRING, 
        department STRING
    )
    USING DELTA
    PARTITIONED BY (process_date TIMESTAMP)
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)

# COMMAND ----------

table_name = 'hired_employees'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.bronze.{table_name} (
        id STRING,
        name STRING,
        datetime STRING,
        department_id STRING,
        job_id STRING
    )
    USING DELTA
    PARTITIONED BY (process_date TIMESTAMP)
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Tables

# COMMAND ----------

table_name = 'jobs'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.silver.{table_name} (
        id int, 
        jobs STRING
    )
    USING DELTA
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)

# COMMAND ----------

table_name = 'departments'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.silver.{table_name} (
        id int, 
        department STRING
    )
    USING DELTA
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)

# COMMAND ----------

table_name = 'hired_employees'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.silver.{table_name} (
        id INTEGER,
        name STRING,
        datetime STRING,
        department_id INTEGER,
        job_id INTEGER
    )
    USING DELTA
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold Tables

# COMMAND ----------

table_name = 'employees_hired_by_quarter'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.gold.{table_name} (
        department string,
        jobs string,
        Q1 long,
        Q2 long,
        Q3 long,
        Q4 long
    )
    USING DELTA
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)

# COMMAND ----------

table_name = 'departments_hired_above_mean'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.gold.{table_name} (
        id integer,
        department string,
        hired long
    )
    USING DELTA
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Log Table

# COMMAND ----------

table_name = 'log_table'

query = f"""
    CREATE TABLE IF NOT EXISTS {hive_schema}.gold.{table_name} (
        task_name STRING,
        status STRING,
        error_msg STRING,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        source_count INT,
        target_count INT
    )
    USING DELTA
""".format(
    hive_schema=hive_schema,
    table_name=table_name
)

print(query)
spark.sql(query)
