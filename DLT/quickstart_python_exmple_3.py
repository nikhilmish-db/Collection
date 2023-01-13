# Databricks notebook source
import dlt

@dlt.view
def taxi_raw():
  return spark.read.format("json").load("/databricks-datasets/nyctaxi/sample/json/")

# Use the function name as the table name
@dlt.table
def filtered_data3():
  return dlt.read("taxi_raw")


# COMMAND ----------

# df = spark.read.format("json").load("/databricks-datasets/nyctaxi/sample/json/")
# display(df)
