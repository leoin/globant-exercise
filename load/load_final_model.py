# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("silver_job_table", "")
dbutils.widgets.text("silver_hired_employees_table", "")
dbutils.widgets.text("silver_departments_table", "")
dbutils.widgets.text("gold_employees_hired_by_quarter", "")
dbutils.widgets.text("gold_departments_hired_above_mean", "")

silver_job_table = dbutils.widgets.get("silver_job_table")
silver_hired_employees_table = dbutils.widgets.get("silver_hired_employees_table")
silver_departments_table = dbutils.widgets.get("silver_departments_table")
gold_employees_hired_by_quarter = dbutils.widgets.get("gold_employees_hired_by_quarter")
gold_departments_hired_above_mean = dbutils.widgets.get("gold_departments_hired_above_mean")


print(f'''
silver_job_table = {silver_job_table}
silver_hired_employees_table = {silver_hired_employees_table}
silver_departments_table = {silver_departments_table}
gold_employees_hired_by_quarter = {gold_employees_hired_by_quarter}
gold_departments_hired_above_mean = {gold_departments_hired_above_mean}
''')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Query 1:
# MAGIC - Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.

# COMMAND ----------

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

print(df.count())
df.write.mode("overwrite").insertInto(gold_employees_hired_by_quarter)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Query 2:
# MAGIC - List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).

# COMMAND ----------

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
        where job_id is not null
        group by department_id

    )
)
'''

print(query)

df = spark.sql(query)
print(df.count())

df.write.mode("overwrite").insertInto(gold_departments_hired_above_mean)
