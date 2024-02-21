# Databricks notebook source
# MAGIC %run ../config/functions

# COMMAND ----------

# MAGIC %run ../config/mapping

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("bronze_table", "")
dbutils.widgets.text("silver_table", "")
dbutils.widgets.text("file", "")

dbutils.widgets.text("task_name", "")
dbutils.widgets.text("log_table", "")



bronze_table = dbutils.widgets.get("bronze_table").lower()
silver_table = dbutils.widgets.get("silver_table").lower()
file = dbutils.widgets.get("file").lower()

task_name = dbutils.widgets.get("task_name")
log_table = dbutils.widgets.get("log_table").lower()



print(f'''
bronze_table = {bronze_table}
silver_table = {silver_table}
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

if(status != 'ERROR'):
    try:

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

    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error using the mappings: '+str(e)

# COMMAND ----------

if(status != 'ERROR'):
    try:
        
        df = spark.sql(f'''
            select * from {bronze_table} 
            where process_date in (select max(process_date) from {bronze_table})
        ''')

        source_count = df.count()
    
    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error reading bronze: '+str(e)

# COMMAND ----------

if(status != 'ERROR'):
    try:
        
        df_silver = spark.table(silver_table)
        df_silver_columns = df_silver.columns
        df_silver_schema = df_silver.schema

        print(df_silver_columns,df_silver_schema)

    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error reading columns and schema from silver: '+str(e)

# COMMAND ----------

if(status != 'ERROR'):
    try:
        
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

    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error applying Transformations: '+str(e)

# COMMAND ----------

if(status != 'ERROR'):
    try:
      
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

    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error in Merge: '+str(e)

# COMMAND ----------

columns = ['task_name','status','error_msg','start_date','end_date','source_count','target_count']
data = [(task_name,status,error_msg,start_date,return_actual_timestamp(),source_count,target_count)]

insert_into_log(data,columns,log_table,status,error_msg)
