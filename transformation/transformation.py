# Databricks notebook source
# MAGIC %run ../config/functions

# COMMAND ----------

# MAGIC %run ../config/mapping

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("bronze_table", "")
dbutils.widgets.text("silver_table", "")
dbutils.widgets.text("file", "")

bronze_table = dbutils.widgets.get("bronze_table")
silver_table = dbutils.widgets.get("silver_table")
file = dbutils.widgets.get("file").lower()


print(f'''
bronze_table = {bronze_table}
silver_table = {silver_table}
file = {file}
''')

# COMMAND ----------

if(file in list(ids_mapping.keys())):
    ids = ids_mapping[file]
    print(ids)
else:
    assert False, "Invalid file parameter"

# Generate dynamically the ids for the merge
c = 0
full_ids = ''
for i in ids:
    if(c == 0):
        full_ids += f'TARGET.{i} = SOURCE.{i}'
    else:
        full_ids += f' AND TARGET.{i} = SOURCE.{i}'
    c+=1
print(full_ids)

# COMMAND ----------

df = spark.sql(f'''
    select * from {bronze_table} 
    where process_date in (select max(process_date) from {bronze_table})
''')

# COMMAND ----------

df_silver = spark.table(silver_table)
df_silver_columns = df_silver.columns
df_silver_schema = df_silver.schema

print(df_silver_columns,df_silver_schema)

# COMMAND ----------

# Select columns that we want
df = df.select(df_silver_columns)


# Cast columns to the correct data type
df = df.selectExpr(
    *[f"CAST({col} AS {field.dataType.typeName()}) AS {col}" for col, field in zip(df_silver_columns, df_silver_schema)]
)

# Deduplicate data
window_spec = Window.partitionBy("id").orderBy("id")

df = df.withColumn(
    "row_number", 
    f.row_number().over(window_spec)
)

df = df.where(
    f.col("row_number") == 1
).drop('row_number')

df.createOrReplaceTempView('temp_data')

# COMMAND ----------

query = f'''
MERGE INTO {silver_table} TARGET 
USING temp_data SOURCE ON {full_ids}

   WHEN MATCHED THEN UPDATE SET * -- UPDATE
   WHEN NOT MATCHED BY TARGET THEN INSERT * -- INSERT
'''

print(query)
df_merge = spark.sql(query)

target_count = df_merge.take(1)[0].num_affected_rows

df_merge.display()
