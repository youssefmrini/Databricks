-- Databricks notebook source
--drop catalog football_UK;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h1> Getting started with Volumes </H1>

-- COMMAND ----------

create catalog  if not exists football_UK;
use catalog football_UK;
create schema if not exists allseasons;
use schema allseasons;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <H3> Create a managed volume </H3>

-- COMMAND ----------

create  VOLUME football_UK.allseasons.dataset

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <h3> Drop a volume </h3>

-- COMMAND ----------

drop volume football_UK.allseasons.dataset

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H3> Create an external volume </H3>

-- COMMAND ----------

CREATE external VOLUME football_UK.allseasons.dataset location "s3://databricks-data-ai-uc-demo/youssefmrini/football";
show volumes;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H3> Grant and Revoke permissions on Volumes <H3>

-- COMMAND ----------

--REVOKE ALL PRIVILEGES on VOLUME  dataset from `youssef.mrini@databricks.com`;
GRANT ALL PRIVILEGES on VOLUME  dataset to `youssef.mrini@databricks.com`;

-- COMMAND ----------

show grants on volume dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC #A Volume is a collection of files cataloged in Unity Catalog. It represents a logical volume of storage in a cloud object storage location and provides capabilities for accessing, storing, governing, and organizing files. Volumes contain directories and files for data stored in any format, including structured, semi-structured, and unstructured data. 
-- MAGIC
-- MAGIC dbutils.fs.cp("/FileStore/tables/england-master" ,"/Volumes/football_uk/allseasons/dataset",True)

-- COMMAND ----------


create table  if not exists games using delta;
ALTER TABLE games SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <h1> Autoloader with Volumes </H1>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaHints", "Round int, Date String, `Team 1` String, `Team 2` string").option("cloudFiles.schemaLocation","dbfs:/hi").load("/Volumes/football_uk/allseasons/dataset")
-- MAGIC df.writeStream.format("delta").trigger(availableNow=True).option("mergeSchema", "true").option("checkpointLocation", "dbfs:/hi").toTable("football_UK.allseasons.games")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1> Query the Data </h1>

-- COMMAND ----------

select * from games;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1> Transform the data</h1>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import split, col
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC
-- MAGIC df=spark.table("football_UK.allseasons.games").drop("_rescued_data").select("*",split(col("FT"),"-").alias("score")).drop("FT").withColumnRenamed("Team 1","Team1").withColumnRenamed("Team 2","Team2")
-- MAGIC tf=df.withColumn("Score_T1", df.score[0]).withColumn("Score_T2", df.score[1]).drop("score").select("*",split(col("Date")," ").alias("Date_con")).drop("date")
-- MAGIC tf2=tf.withColumn("DayOfWeek", tf.Date_con[0]).withColumn("Month", tf.Date_con[1]).withColumn("Day", tf.Date_con[2]).withColumn("Year", tf.Date_con[3]).drop("Date_con")
-- MAGIC tf2.write.mode("overwrite").saveAsTable("football_UK.allseasons.games_cleaned")

-- COMMAND ----------

select * from football_UK.allseasons.games_cleaned;
