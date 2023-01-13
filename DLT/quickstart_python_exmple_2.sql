-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE taxi_raw2
AS SELECT * FROM json.`/databricks-datasets/nyctaxi/sample/json/`


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE filtered_data23
AS SELECT * FROM LIVE.taxi_raw
