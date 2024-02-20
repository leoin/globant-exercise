# Databricks notebook source
# MAGIC %run ../config/functions

# COMMAND ----------

# MAGIC %run ../config/mapping

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("data_source_path", "")
dbutils.widgets.text("bronze_table", "")
dbutils.widgets.text("file", "")

data_source_path = dbutils.widgets.get("data_source_path")
bronze_table = dbutils.widgets.get("bronze_table")
file = dbutils.widgets.get("file").lower()


print(f'''
data_source_path = {data_source_path}
bronze_table = {bronze_table}
file = {file}
''')

# COMMAND ----------

if(file in list(custom_schema.keys())):
    schema = custom_schema[file]
    print(schema)
else:
    assert False, "Invalid file parameter"

# COMMAND ----------

df = spark.read.csv(data_source_path, schema=schema)

df = df.withColumn(
    'process_date',
    f.now()
)

df.write.mode("append").insertInto(bronze_table)

# COMMAND ----------

df.show()
