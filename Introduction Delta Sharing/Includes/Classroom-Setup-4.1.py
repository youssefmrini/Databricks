# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

lesson_config.name = 'delta-sharing-best-practices'
lesson_config.create_schema = True

DA = DBAcademyHelper(course_config, lesson_config)  # Create the DA object
DA.reset_lesson()                                   # Reset the lesson to a clean state
DA.init()                                           # Performs basic intialization including creating schemas and catalogs
DA.conclude_setup()                                 # Finalizes the state and prints the config for the student

# COMMAND ----------

DA.paths.flights_dataset = f"{DA.paths.datasets}/flights/flights-01.csv"

df = spark.read.csv(DA.paths.flights_dataset, header="true", inferSchema="true")

spark.sql(f"USE CATALOG {DA.catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS db_flights COMMENT 'Flights dataset'")
spark.sql("USE SCHEMA db_flights")
df.write.partitionBy("origin").mode('overwrite').saveAsTable("flights")

DA.database_name = "db_flights"
DA.table_name = "flights"
