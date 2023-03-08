# Databricks notebook source
from delta import *
df=spark.read.format("delta").load("dbfs:/user/hive/warehouse/predictions")
df.createOrReplaceTempView("donne")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as sum from donne
