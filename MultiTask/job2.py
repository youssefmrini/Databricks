# Databricks notebook source
dbutils.fs.ls("dbfs:/user/hive/warehouse/predictions")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC optimize delta.`dbfs:/user/hive/warehouse/predictions` zorder by predictions
