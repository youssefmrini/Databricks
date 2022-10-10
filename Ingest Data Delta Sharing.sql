-- Databricks notebook source
SET spark.databricks.delta.formatCheck.enabled=false

-- COMMAND ----------

-- MAGIC 
-- MAGIC %python
-- MAGIC df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://songkun-uc-external-2@songkunucexternal.dfs.core.windows.net/MutualFund.csv")
-- MAGIC df.write.saveAsTable("mutualfund")

-- COMMAND ----------

--create catalog demo_deltasharing;
use catalog demo_deltasharing;
--create database ingestion;
use ingestion;

--create table mutualfund using delta as select * from csv."abfss://songkun-uc-external-2@songkunucexternal.dfs.core.windows.net/MutualFund.csv" 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.sql("select quarter(price_date) as quarter, year(price_date) as year, month(price_date) as month, to_date(price_date) as date, fund_symbol, nav_per_share from mutualfund")
-- MAGIC df.write.mode("overwrite").partitionBy("year","quarter","month").saveAsTable("mutual_fund")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.sql("select quarter(price_date) as quarter, year(price_date) as year, month(price_date) as month, to_date(price_date) as date, fund_symbol, nav_per_share from mutualfund")
-- MAGIC df.write.mode("overwrite").saveAsTable("mutual_fund_part")

-- COMMAND ----------

describe extended mutual_fund

-- COMMAND ----------

ALTER TABLE mutual_fund SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

select * from mutual_fund

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType, DoubleType
-- MAGIC 
-- MAGIC 
-- MAGIC data2 = [(2,1974,8,"1974-08-01","ACGIX",5.62),
-- MAGIC     (2,1974,8,"1974-08-02","ACGIX",5.0),
-- MAGIC     (3,1974,9,"1974-08-03","ACGIX",4000.0),
-- MAGIC     (3,1974,10,"1974-08-04","ACGIX",4000.0),
-- MAGIC     (4,1974,8,"1974-08-05","ACGIX",1.0)]
-- MAGIC 
-- MAGIC df = spark.createDataFrame(data2,["quarter","year","month","date","fund_symbol","nav_per_share"])
-- MAGIC df.createOrReplaceTempView("updates")
-- MAGIC display(df)

-- COMMAND ----------

merge into mutual_fund using updates 
on mutual_fund.date=updates.date and mutual_fund.fund_symbol=updates.fund_symbol
WHEN MATCHED THEN UPDATE SET *
