-- Databricks notebook source
--create catalog demo_delta;
use catalog demo_delta;
--create schema example;
use schema example;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.option("header","true").option("sep",",").format("csv").load("/Volumes/demo_delta/demo/you/yellow_tripdata_2016-03.csv")
-- MAGIC df1 = df.withColumn("trip_distance", df["trip_distance"].cast("float")).withColumn("passenger_count", df["passenger_count"].cast("int")).withColumn("fare_amount", df["fare_amount"].cast("float")).withColumn("extra", df["extra"].cast("float")).withColumn("tolls_amount", df["tolls_amount"].cast("float")).withColumn("improvement_surcharge", df["improvement_surcharge"].cast("float"))
-- MAGIC
-- MAGIC # Display the DataFrame to verify the changes
-- MAGIC #display(df1)
-- MAGIC df1.write.saveAsTable("demo_delta.example.rawdata")
-- MAGIC display(df1)

-- COMMAND ----------

-- DBTITLE 1,Deep clone to have the same features
create table demo_delta.example.rawdata_widening deep clone demo_delta.example.rawdata

-- COMMAND ----------

-- DBTITLE 1,View the Min readerVersion and writerversion for the table rawdata
describe detail demo_delta.example.rawdata


-- COMMAND ----------

-- DBTITLE 1,View the Min readerVersion and writerversion for the table rawdata_widening
describe detail demo_delta.example.rawdata_widening

-- COMMAND ----------

-- DBTITLE 1,Enable Type Widening which upgrade the protocol at the same time
  ALTER TABLE demo_delta.example.rawdata_widening SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')


-- COMMAND ----------

-- DBTITLE 1,Verify the  new versions of the protocol
describe detail demo_delta.example.rawdata_widening

-- COMMAND ----------

-- DBTITLE 1,Change the type of the column
ALTER TABLE demo_delta.example.rawdata_widening ALTER COLUMN trip_distance TYPE double


-- COMMAND ----------

-- DBTITLE 1,View the change
describe extended demo_delta.example.rawdata_widening

-- COMMAND ----------

-- DBTITLE 1,Optimize the table
optimize rawdata_widening;

-- COMMAND ----------

-- DBTITLE 1,Deep clone the table to have the same features and table size
create table demo_delta.example.rawdata_widening_clustering deep clone demo_delta.example.rawdata_widening

-- COMMAND ----------

select * from demo_delta.example.rawdata_widening where VendorID=1 and payment_type=3

-- COMMAND ----------

-- DBTITLE 1,Add Custering Key
alter table demo_delta.example.rawdata_widening_clustering cluster by (VendorID,payment_type);
optimize demo_delta.example.rawdata_widening_clustering ;

-- COMMAND ----------

-- DBTITLE 1,Query the table with the Same where clause
select * from demo_delta.example.rawdata_widening_clustering where VendorID=1 and payment_type=3
