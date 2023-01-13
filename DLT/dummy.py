# Databricks notebook source
from multiprocessing.pool import ThreadPool
import time

## Change the database name of your choice
databse_name = 'nmishra'
#create a list of all tables to generate metrics
table_list = spark.sql(f"show tables in {databse_name}").select('database','tablename').collect()
print(table_list)


def check_counts(row):
  res_cnt = spark.table(f"{row.database}.{row.tablename}").count()
  insert_vals = f"('{row.tablename} records: ',{res_cnt})"
  return insert_vals

start_time = time.time()
#Establish Fan out for table list with threads equivalent to number of table 
pool = ThreadPool(len(table_list))
#Check the total number of records in all tables
check_count_res = pool.map(check_counts, table_list)
end_time = time.time()
print(f"{','.join(list(check_count_res))}")
print(f'check counts complete in {end_time-start_time}')

# COMMAND ----------


