-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <h1> Dbutils </H1>
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.help()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls("/user/hive/warehouse")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H1> Hive </H1>
-- MAGIC

-- COMMAND ----------

show databases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls("dbfs:/databricks-datasets/amazon/data20K")

-- COMMAND ----------

create database if not exists hivedemoyoussef

-- COMMAND ----------

use hivedemoyoussef

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #dbutils.fs.mkdirs("dbfs:/youssefmrini")
-- MAGIC #dbutils.fs.rm("dbfs:/youssefmrini", recurse=True)
-- MAGIC dbutils.fs.cp("dbfs:/databricks-datasets/amazon/data20K","dbfs:/youssefmrini/amazon", recurse=True)
-- MAGIC #dbutils.fs.cp("dbfs:/databricks-datasets/amazon/data20K","dbfs:/youssefmrini/amazondelta", recurse=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.ls("dbfs:/youssefmrini")

-- COMMAND ----------

create table if not exists hivedemoyoussef.amazon USING parquet location 'dbfs:/youssefmrini/amazon'

-- COMMAND ----------

select * from hivedemoyoussef.amazon

-- COMMAND ----------

describe detail hivedemoyoussef.amazon


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.sql("show tables from hivedemoyoussef")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1> Managed and External Delta Table</h1>
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS ExternelDelta;

CREATE TABLE if not exists ExternelDelta
USING DELTA 
location "dbfs:/youssefmrini/amazonexternales"
AS (SELECT * FROM hivedemoyoussef.amazon)

-- COMMAND ----------

describe extended externeldelta

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.table("ExternelDelta")
-- MAGIC df.write.format("delta").mode("overwrite").option("overwriteSchema",True).partitionBy("rating").save("dbfs:/youssefmrini/amazonexternales")

-- COMMAND ----------

describe extended ExternelDelta

-- COMMAND ----------

DROP TABLE IF EXISTS ManagedDelta;

CREATE TABLE ManagedDelta
USING DELTA 
AS (SELECT * FROM hivedemoyoussef.amazon)

-- COMMAND ----------

describe extended ManagedDelta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Change the order of the columns and Insert columns in a specific order </H2> <br>
-- MAGIC
-- MAGIC <a href="https://docs.databricks.com/delta/delta-batch.html#explicitly-update-schema"> Documentation </a>
-- MAGIC

-- COMMAND ----------

alter table ManagedDelta alter column rating  after review;
alter table ManagedDelta add columns comments string after rating;

-- COMMAND ----------

describe  ManagedDelta

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2>Delta column mapping </h2>
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.format("csv").option("header","true").option("sep",";").load("/FileStore/carss.csv")
-- MAGIC df.createOrReplaceTempView("datacar")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("delta").saveAsTable("carInfos")

-- COMMAND ----------

create or replace table carinfos using delta 
  TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
)
as select * from datacar

-- COMMAND ----------

select * from carinfos

-- COMMAND ----------

ALTER TABLE carinfos RENAME COLUMN `Car details` TO car

-- COMMAND ----------

select * from carinfos

-- COMMAND ----------

-- MAGIC %md <h1> Caching </h1>
-- MAGIC <H3>Fast query due to delta caching!</H3>
-- MAGIC There are two types of caching available in Databricks: Delta caching and Spark caching. Here are the characteristics of each type:
-- MAGIC <ul>
-- MAGIC <li>Type of stored data: The Delta cache contains local copies of remote data. It can improve the performance of a wide range of queries, but cannot be used to store results of arbitrary subqueries. The Spark cache can store the result of any subquery data and data stored in formats other than Parquet (such as CSV, JSON, and ORC).</li>
-- MAGIC <li>Performance: The data stored in the Delta cache can be read and operated on faster than the data in the Spark cache.</li>
-- MAGIC <li>Automatic vs manual control: When the Delta cache is enabled, data that has to be fetched from a remote source is automatically added to the cache. This process is fully transparent and does not require any action. However, to preload data into the cache beforehand, you can use the CACHE command (see Cache a subset of the data). When you use the Spark cache, you must manually specify the tables and queries to cache.</li>
-- MAGIC <li>Disk vs memory-based: The Delta cache is stored on the local disk, so that memory is not taken away from other operations within Spark. Due to the high read speeds of modern SSDs, the Delta cache can be fully disk-resident without a negative impact on its performance. In contrast, the Spark cache uses memory.</li>
-- MAGIC </ul>  
-- MAGIC
-- MAGIC <h4> The Delta cache works for all Parquet files and is not limited to Delta Lake format files. </h4>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.io.cache.enabled", "true")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC cache select * from ManagedDelta

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("ManagedDelta")
-- MAGIC df.cache().count()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Mounts </H2>
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %fs mounts

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC use hivedemoyoussef;
-- MAGIC drop table ExternelDelta;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.read.format("delta").load("dbfs:/youssefmrini/amazonexternales")
-- MAGIC display(df)

-- COMMAND ----------

DROP TABLE IF EXISTS ExternelDelta;

CREATE TABLE if not exists ExternelDelta
USING DELTA 
location "dbfs:/youssefmrini/amazonexternales"

-- COMMAND ----------

select * from ExternelDelta;
