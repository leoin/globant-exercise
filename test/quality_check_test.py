# Databricks notebook source
# MAGIC %md
# MAGIC ## Prejob
# MAGIC - Import functions
# MAGIC - Get parameters

# COMMAND ----------

# MAGIC %run ../config/functions

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("bronze_job_table", "")
dbutils.widgets.text("bronze_hired_employees_table", "")
dbutils.widgets.text("bronze_departments_table", "")

dbutils.widgets.text("silver_job_table", "")
dbutils.widgets.text("silver_hired_employees_table", "")
dbutils.widgets.text("silver_departments_table", "")

dbutils.widgets.text("qc_table", "")



silver_job_table = dbutils.widgets.get("silver_job_table").lower()
silver_hired_employees_table = dbutils.widgets.get("silver_hired_employees_table").lower()
silver_departments_table = dbutils.widgets.get("silver_departments_table").lower()

bronze_job_table = dbutils.widgets.get("bronze_job_table").lower()
bronze_hired_employees_table = dbutils.widgets.get("bronze_hired_employees_table").lower()
bronze_departments_table = dbutils.widgets.get("bronze_departments_table").lower()

qc_table = dbutils.widgets.get("qc_table").lower()



print(f'''
silver_job_table = {silver_job_table}
silver_hired_employees_table = {silver_hired_employees_table}
silver_departments_table = {silver_departments_table}

bronze_job_table = {bronze_job_table}
bronze_hired_employees_table = {bronze_hired_employees_table}
bronze_departments_table = {bronze_departments_table}

qc_table = {qc_table}
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read all tables from Bronze and Silver
# MAGIC - For the tables of bronze apply window function in order to get the last record in order to compare to the merge
# MAGIC - For the tables of silver just read them

# COMMAND ----------

# Deduplicate data
window_spec = Window.partitionBy("id").orderBy("id")


df_bron_job = spark.table(bronze_job_table)
df_bron_job = get_last_record(df_bron_job,window_spec)

df_bron_dep = spark.table(bronze_departments_table)
df_bron_dep = get_last_record(df_bron_dep,window_spec)

df_bron_hir = spark.table(bronze_hired_employees_table)
df_bron_hir = get_last_record(df_bron_hir,window_spec)


df_silv_job = spark.table(silver_job_table)
df_silv_dep = spark.table(silver_departments_table)
df_silv_hir = spark.table(silver_hired_employees_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## QC Tests

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Check if there are duplicates in Silver

# COMMAND ----------

job_count = df_silv_job.groupBy('id').count().where(f.col('count') > 1).count()
dep_count = df_silv_dep.groupBy('id').count().where(f.col('count') > 1).count()
hir_count = df_silv_hir.groupBy('id').count().where(f.col('count') > 1).count()

results_dict = {
    'job_count':job_count,
    'dep_count':dep_count,
    'hir_count':hir_count
}

conclusion = (job_count == 0 and dep_count == 0 and hir_count == 0)

columns = ['name_of_test','tables','time_of_test','results','conclusion']
data = [('Duplicates of records in Silver','All silver tables',return_actual_timestamp(),str(results_dict),str(conclusion))]

insert_into_qc_table(data,columns,qc_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Check if the counts between bronze to silver

# COMMAND ----------

brz_job_count = df_bron_job.count()
sil_job_count = df_silv_job.count()
brz_dep_count = df_bron_dep.count()
sil_dep_count = df_silv_dep.count()
brz_hir_count = df_bron_hir.count()
sil_hir_count = df_silv_hir.count()

results_dict = {
    'brz_job_count':brz_job_count,
    'sil_job_count':sil_job_count,
    'brz_dep_count':brz_dep_count,
    'sil_dep_count':sil_dep_count,
    'brz_hir_count':brz_hir_count,
    'sil_hir_count':sil_hir_count
}

conclusion = (brz_job_count == sil_job_count and brz_dep_count == sil_dep_count and brz_hir_count == sil_hir_count)


columns = ['name_of_test','tables','time_of_test','results','conclusion']
data = [('Counts of records between Bronze and Silver','All bronze and silver tables',return_actual_timestamp(),str(results_dict),str(conclusion))]

insert_into_qc_table(data,columns,qc_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Check the transformations after bronze in silver

# COMMAND ----------

results_dict = {}
conclusion_count = 0


def return_nulls_count(table,brz_table,sil_table,results_dict,conclusion_count):
    columns = sil_table.columns

    for c in columns:
        df_brz_temp_count = brz_table.select(f.col(c)).where(f.col(c).isNull()).count()
        df_sil_temp_count = sil_table.select(f.col(c)).where(f.col(c).isNull()).count()
        
        results_dict[table+'_'+c] = {
            'bronze':df_brz_temp_count,
            'silver':df_sil_temp_count
        }
        conclusion_count += 0 if df_brz_temp_count == df_sil_temp_count else 1
    return results_dict,conclusion_count


jobs_results = return_nulls_count('jobs',df_bron_job,df_silv_job,results_dict,conclusion_count)
depa_results = return_nulls_count('department',df_bron_dep,df_silv_dep,jobs_results[0],jobs_results[1])
hire_results = return_nulls_count('hire',df_bron_hir,df_silv_hir,depa_results[0],depa_results[1])




results_dict = hire_results[0]
conclusion = hire_results[1] == 0


columns = ['name_of_test','tables','time_of_test','results','conclusion']
data = [('Analyze if the transformation works in Silver','All bronze and silver tables',return_actual_timestamp(),str(results_dict),str(conclusion))]

insert_into_qc_table(data,columns,qc_table)
