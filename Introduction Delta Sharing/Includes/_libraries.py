# Databricks notebook source
# VALIDATE_LIBRARIES

# COMMAND ----------

#version = spark.conf.get("dbacademy.library.version", "v3.0.0")
version = spark.conf.get("dbacademy.library.version", "v2.0.12")
default_command = f"install --quiet --disable-pip-version-check git+https://github.com/databricks-academy/dbacademy@{version}"
pip_command = spark.conf.get("dbacademy.library.install", default_command)
if pip_command != default_command: print(f"WARNING: Using alternative library installation:\n| default: %pip {default_command}\n| current: %pip {pip_command}")

# COMMAND ----------

# MAGIC %pip $pip_command
