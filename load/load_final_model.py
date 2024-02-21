# Databricks notebook source
# MAGIC %run ../config/functions

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("silver_job_table", "")
dbutils.widgets.text("silver_hired_employees_table", "")
dbutils.widgets.text("silver_departments_table", "")
dbutils.widgets.text("gold_employees_hired_by_quarter", "")
dbutils.widgets.text("gold_departments_hired_above_mean", "")

dbutils.widgets.text("task_name", "")
dbutils.widgets.text("log_table", "")

silver_job_table = dbutils.widgets.get("silver_job_table").lower()
silver_hired_employees_table = dbutils.widgets.get("silver_hired_employees_table").lower()
silver_departments_table = dbutils.widgets.get("silver_departments_table").lower()
gold_employees_hired_by_quarter = dbutils.widgets.get("gold_employees_hired_by_quarter").lower()
gold_departments_hired_above_mean = dbutils.widgets.get("gold_departments_hired_above_mean").lower()

task_name = dbutils.widgets.get("task_name")
log_table = dbutils.widgets.get("log_table").lower()


print(f'''
silver_job_table = {silver_job_table}
silver_hired_employees_table = {silver_hired_employees_table}
silver_departments_table = {silver_departments_table}
gold_employees_hired_by_quarter = {gold_employees_hired_by_quarter}
gold_departments_hired_above_mean = {gold_departments_hired_above_mean}

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
# MAGIC
# MAGIC ### Query 1:
# MAGIC - Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.

# COMMAND ----------

if(status != 'ERROR'):
    try:

        query = f'''
        select

            department,
            jobs,
            sum(if(quarter == 1,1,0)) as Q1,
            sum(if(quarter == 2,1,0)) as Q2,
            sum(if(quarter == 3,1,0)) as Q3,
            sum(if(quarter == 4,1,0)) as Q4

        from(

            select
                h.*,
                d.department,
                j.jobs,
                quarter(h.datetime) as quarter
            from {silver_hired_employees_table} as h
            inner join {silver_job_table} as j on h.job_id = j.id
            inner join {silver_departments_table} as d on h.department_id = d.id
            where h.job_id is not null
        )

        where year(datetime) = '2021'
        group by department,jobs
        order by department,jobs asc
        '''

        print(query)

        df = spark.sql(query)

        source_count += df.count()

        df.write.mode("overwrite").insertInto(gold_employees_hired_by_quarter)

        target_count += df.count()

    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error in Query 1: '+str(e)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Query 2:
# MAGIC - List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).

# COMMAND ----------

if(status != 'ERROR'):
    try:

        query = f'''

        select
        *
        from(
            select

                department_id as id,
                department,
                count(*) as hired

            from(

                select
                    h.*,
                    d.department
                from {silver_hired_employees_table} as h
                inner join {silver_departments_table} as d on h.department_id = d.id
                where h.job_id is not null

            )

            where year(datetime) = '2021'
            group by department_id,department
            order by hired desc
        )
        where hired > (
            select
                mean(count) as mean
            from(

                select
                    department_id,
                    count(*) as count
                from {silver_hired_employees_table}
                where job_id is not null and year(datetime) = '2021'
                group by department_id

            )
        )
        '''

        print(query)

        df = spark.sql(query)

        source_count += df.count()

        df.write.mode("overwrite").insertInto(gold_departments_hired_above_mean)

        target_count += df.count()

    except Exception as e:
        status = 'ERROR'
        error_msg = 'Error in Query 2: '+str(e)

# COMMAND ----------

columns = ['task_name','status','error_msg','start_date','end_date','source_count','target_count']
data = [(task_name,status,error_msg,start_date,return_actual_timestamp(),source_count,target_count)]

insert_into_log(data,columns,log_table,status,error_msg)
