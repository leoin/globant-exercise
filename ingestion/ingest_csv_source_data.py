# Databricks notebook source
# MAGIC %md
# MAGIC ## Prejob
# MAGIC - Import functions and mappings
# MAGIC - Get parameters
# MAGIC - Define default values for the log

# COMMAND ----------

# MAGIC %run ../config/functions

# COMMAND ----------

# MAGIC %run ../config/mapping

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("data_source_path", "")
dbutils.widgets.text("bronze_table", "")
dbutils.widgets.text("file", "")

dbutils.widgets.text("task_name", "")
dbutils.widgets.text("log_table", "")



data_source_path = dbutils.widgets.get("data_source_path").lower()
bronze_table = dbutils.widgets.get("bronze_table").lower()
file = dbutils.widgets.get("file").lower()

task_name = dbutils.widgets.get("task_name")
log_table = dbutils.widgets.get("log_table").lower()



print(f'''
data_source_path = {data_source_path}
bronze_table = {bronze_table}
file = {file}

task_name = {task_name}
log_table = {log_table}
''')

# COMMAND ----------

status = 'Success'
error_msg = 'All run OK'
start_date = return_actual_timestamp()
source_count = 0
target_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get schema from mapping

# COMMAND ----------

if(status != 'ERROR'):
    try:
        if(file in list(custom_schema.keys())):
            schema = custom_schema[file]
            print(schema)
        else:
            msg_error = "Invalid file parameter"
            assert False, msg_error
    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error in schema: '+msg_error

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read data Source and append in bronze table

# COMMAND ----------

if(status != 'ERROR'):
    try:
        df = spark.read.csv(data_source_path, schema=schema)

        source_count = df.count()

        df = df.withColumn(
            'process_date',
            f.now()
        )

        df.write.mode("append").insertInto(bronze_table)
        target_count = df.count()
    
    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error reading Source: '+str(e)
        target_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Insert results into the Log

# COMMAND ----------

columns = ['task_name','status','error_msg','start_date','end_date','source_count','target_count']
data = [(task_name,status,error_msg,start_date,return_actual_timestamp(),source_count,target_count)]

insert_into_log(data,columns,log_table,status,error_msg)
