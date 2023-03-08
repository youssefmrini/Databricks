# Databricks notebook source
diamonds=spark.read.option("header","true").format("csv").load("dbfs:/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv").drop("_c0")

# COMMAND ----------

import pandas as pd
diamonds=pd.read_csv("/dbfs/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
display(diamonds)

# COMMAND ----------

display(diamonds)

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets
