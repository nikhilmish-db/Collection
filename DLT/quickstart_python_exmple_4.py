# Databricks notebook source
import dlt
@dlt.table
def wiki_raw():
  return (
    spark.readStream.format("cloudFiles")
      .schema("title STRING, id INT, revisionId INT, revisionTimestamp TIMESTAMP, revisionUsername STRING, revisionUsernameId INT, text STRING")
      .option("cloudFiles.format", "parquet")
      .load("/databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet")
  )
