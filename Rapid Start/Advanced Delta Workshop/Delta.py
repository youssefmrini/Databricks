# Databricks notebook source

displayHTML("<img src='https://databricks.com/wp-content/uploads/2019/08/Delta-Lake-Multi-Hop-Architecture-Bronze.png'>")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up the environment

# COMMAND ----------

dbutils.widgets.text("team_name", "Enter your team's name");
team_name = dbutils.widgets.get("team_name")
setup_responses = dbutils.notebook.run("./includes/flight_school_assignment_1_setup", 0, {"team_name": team_name}).split()
local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print(f"Path to be used for Local Files: {local_data_path}")
print(f"Path to be used for DBFS Files: {dbfs_data_path}")
print(f"Database Name: {database_name}")

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Sensor Data
# MAGIC We'll read the raw data into a __Dataframe__.  The dataframe is a key structure in Apache Spark.  It is an in-memory data structure in a rows-and-columns format that is very similar to a relational database table.  In fact, we'll be creating SQL Views against the dataframes so that we can manipulate them using standard SQL.

# COMMAND ----------

#dbutils.fs.rm("/FileStore/_delta_log",True)

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "false")

# COMMAND ----------

import pyspark.sql.types as T   


dataPath = f"dbfs:/FileStore/flight/{team_name}/assignment_1_ingest.csv"

schemas=T.StructType([
  T.StructField("id", T.StringType(), True),
  T.StructField("reading_time", T.TimestampType(), True),
  T.StructField("device_type", T.StringType(), True),
  T.StructField("device_id", T.StringType(), True),
  T.StructField("device_operational_status", T.StringType(), True),
  T.StructField("reading_1", T.DoubleType(), True),
  T.StructField("reading_2", T.DoubleType(), True),
  T.StructField("reading_3", T.DoubleType(), True),
])

df = (spark
      .read
      .option("header", True)
      .schema(schemas)
      .csv(dataPath)
     )

df.createOrReplaceTempView("info")

display(df)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create SQL view
# MAGIC In order to be able to query the data using sql we create a view of the dataframe.

# COMMAND ----------

df.createOrReplaceTempView("info")

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks allows to use different languages inside the same notebook by using magic commands. Below we use the ``%sql`` command to write a sql query. You can also exclude colmuns using Except

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * except (id) FROM info

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Backfill Data
# MAGIC
# MAGIC Some backfill sensor data (e.g. late arriving or corrected) is provided which we eventually need to merge with the original data.

# COMMAND ----------

# Read the downloaded backfill data into a dataframe
# This is some backfill data that we'll need to merge into the main historical data.  

dataPath = f"dbfs:/FileStore/flight/{team_name}/assignment_1_backfill.csv"

df_backfill = (spark
               .read
               .option("header", True)
               .option("inferSchema", True)
               .csv(dataPath)
              )

display(df_backfill)

# COMMAND ----------

df.createOrReplaceTempView("historical_bronze_vw")
df_backfill.createOrReplaceTempView("historical_bronze_backfill_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Historical Tables
# MAGIC
# MAGIC - Ingest raw data into Bronze table
# MAGIC - Use Delta for all Bronze/Silver/Gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Working with Delta tables
# MAGIC We can use standard SQL to work with delta tables. To create a Delta table we simply add ``USING DELTA`` to the SQL statement.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a Delta Lake table for the main bronze table
# MAGIC
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_bronze;
# MAGIC
# MAGIC CREATE TABLE sensor_readings_historical_bronze
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC    delta.dataSkippingNumIndexedCols = 0,
# MAGIC    delta.logRetentionDuration = "interval 1 days",
# MAGIC    delta.appendOnly=true
# MAGIC   )
# MAGIC   AS (SELECT * FROM historical_bronze_vw)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sensor_readings_historical_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC delete from sensor_readings_historical_bronze where id="34fb2d8a-5829-4036-adea-a08ccc2c260c"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC alter table sensor_readings_historical_bronze set tblproperties (   delta.appendOnly=false, 'delta.columnMapping.mode' = 'name',
# MAGIC    'delta.minReaderVersion' = '2',
# MAGIC    'delta.minWriterVersion' = '5');
# MAGIC show tblproperties sensor_readings_historical_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC alter table sensor_readings_historical_bronze drop column if exists reading_2;
# MAGIC select * from sensor_readings_historical_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC You can use ALTER TABLE <table_name> DROP COLUMN [IF EXISTS] <column_name> or ALTER TABLE <table_name> DROP COLUMNS [IF EXISTS] (<column_name>, *) to drop a column or a list of columns, respectively, from a Delta table as a metadata-only operation. The columns are effectively “soft-deleted,” as they are still in the underlying Parquet files but are no longer visible to the Delta table.
# MAGIC
# MAGIC You can use REORG TABLE <table_name> APPLY (PURGE) to trigger a file rewrite on the files that contain any soft-deleted data such as dropped columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC REORG TABLE sensor_readings_historical_bronze APPLY (PURGE)

# COMMAND ----------

from delta import *

DeltaTable.createIfNotExists(spark) \
  .tableName("default.sensor_readings_historical_bronze_python") \
  .addColumn("id", "STRING") \
  .addColumn("reading_time", "TIMESTAMP") \
  .addColumn("device_type", "STRING") \
  .addColumn("device_id", "STRING", comment="identity of the device") \
  .addColumn("device_operational_status", "STRING") \
  .addColumn("reading_1", "FLOAT") \
  .addColumn("reading_2", "FLOAT") \
  .addColumn("reading_3", "FLOAT") \
  .execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended default.sensor_readings_historical_bronze_python

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explore Bronze table
# MAGIC
# MAGIC The following few cells demonstrate how tables and data can be explored easily within Databricks.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's make a query that shows a meaningful graphical view of the table
# MAGIC -- How many rows exist for each operational status?
# MAGIC -- Experiment with different graphical views... be creative!
# MAGIC SELECT 
# MAGIC device_operational_status, device_type, count(device_id) as device_count
# MAGIC FROM sensor_readings_historical_bronze
# MAGIC GROUP BY device_operational_status, device_type
# MAGIC ORDER BY device_count

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver table
# MAGIC
# MAGIC  Let's deal with them and create a clean Silver table.
# MAGIC
# MAGIC Process: 
# MAGIC - Create Delta table based on Bronze data
# MAGIC - Merge backfill data into new silver table
# MAGIC - Remove invalid data
# MAGIC
# MAGIC #### Create table & Merge Backfill
# MAGIC
# MAGIC - Create the silver table based on Bronze data, merge the backfill data using __MERGE INTO__. This would not be possible with standard tables.
# MAGIC - Explore the new table and show the updated records

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Let's create a Silver table.  We'll start with the Bronze data, then make several improvements
# MAGIC
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver;
# MAGIC
# MAGIC CREATE TABLE sensor_readings_historical_silver 
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC    delta.dataSkippingNumIndexedCols = 8,
# MAGIC    delta.autoOptimize.optimizeWrite = true,
# MAGIC    delta.logRetentionDuration = "interval 16 days",
# MAGIC    delta.deletedFileRetentionDuration = "interval 16 days",
# MAGIC    delta.enableChangeDataFeed = true
# MAGIC   )
# MAGIC AS (SELECT * FROM sensor_readings_historical_bronze)

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended sensor_readings_historical_silver reading_time;

# COMMAND ----------

# MAGIC  %sql
# MAGIC  
# MAGIC ANALYZE TABLE sensor_readings_historical_silver COMPUTE STATISTICS FOR all COLUMNS;
# MAGIC desc extended sensor_readings_historical_silver reading_time;

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>Low Shuffle Merge provides better performance by processing unmodified rows in a separate, more streamlined processing mode, instead of processing them together with the modified rows. As a result, the amount of shuffled data is reduced significantly, leading to improved performance. Low Shuffle Merge also removes the need for users to re-run the OPTIMIZE ZORDER BY command after performing a MERGE operation. For the data that has already been sorted (using OPTIMIZE Z-ORDER BY), Low Shuffle Merge maintains that sorting for all records that are not being modified by the MERGE command. These improvements save significant time and compute costs.
# MAGIC <h4/>

# COMMAND ----------

spark.conf.set("spark.databricks.delta.merge.enableLowShuffle","true")


# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) MERGE INTO Support
# MAGIC
# MAGIC #### INSERT or UPDATE parquet: 7-step process
# MAGIC
# MAGIC With a legacy data pipeline, to insert or update a table, you must:
# MAGIC 1. Identify the new rows to be inserted
# MAGIC 2. Identify the rows that will be replaced (i.e. updated)
# MAGIC 3. Identify all of the rows that are not impacted by the insert or update
# MAGIC 4. Create a new temp based on all three insert statements
# MAGIC 5. Delete the original table (and all of those associated files)
# MAGIC 6. "Rename" the temp table back to the original table name
# MAGIC 7. Drop the temp table
# MAGIC
# MAGIC ![](https://pages.databricks.com/rs/094-YMS-629/images/merge-into-legacy.gif)
# MAGIC
# MAGIC
# MAGIC #### INSERT or UPDATE with Delta Lake
# MAGIC
# MAGIC 2-step process: 
# MAGIC 1. Identify rows to insert or update
# MAGIC 2. Use `MERGE`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sql Version
# MAGIC
# MAGIC MERGE INTO sensor_readings_historical_silver AS target
# MAGIC USING historical_bronze_backfill_vw AS source
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED  THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC
# MAGIC --Python Version
# MAGIC /*
# MAGIC from delta.tables import *
# MAGIC dft=DeltaTable.forName(spark, "sensor_readings_historical_silver")
# MAGIC
# MAGIC dft.alias("target").merge(
# MAGIC     df_backfill.alias("source"),
# MAGIC     "target.id = source.id") \
# MAGIC   .whenMatchedUpdateAll() \
# MAGIC   .whenNotMatchedInsertAll() \
# MAGIC   .execute()
# MAGIC   */

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h3>To improve query speed, Delta Lake on Databricks supports the ability to optimize the layout of data stored in cloud storage. Delta Lake on Databricks supports two layout algorithms: bin-packing and Z-Ordering </h3>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC OPTIMIZE sensor_readings_historical_silver ZORDER BY reading_time, device_operational_status

# COMMAND ----------

# MAGIC %md
# MAGIC #### Remove invalid records
# MAGIC - Replace all reading = 999.99 with average of the previous and following readings
# MAGIC - Demonstrate that all invalid readings were replaced

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MegaCorp just informed us of some dirty data.  Occasionally they would receive garbled data.
# MAGIC -- In those cases, they would put 999.99 in the readings.
# MAGIC -- Let's find these records
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %md ###![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) DELETE Support

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- SQL version
# MAGIC DELETE FROM sensor_readings_historical_silver where reading_1=999.99; 

# COMMAND ----------

from delta import *
deltaTable = DeltaTable.forName(spark, "sensor_readings_historical_silver")
deltaTable.delete("reading_1=999.99") 


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM sensor_readings_historical_silver WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <H2> Change Data Feed </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe history sensor_readings_historical_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from table_changes("sensor_readings_historical_silver",1,3) 
# MAGIC
# MAGIC
# MAGIC --python version
# MAGIC --df= spark.read.format("delta").option("readChangeData", True).option("startingVersion", 1).table('sensor_readings_historical_silver')
# MAGIC --display(df)

# COMMAND ----------

# MAGIC %md ## ![Delta Lake Tiny Logo](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Delta Lake Time Travel
# MAGIC Delta Lake’s time travel capabilities simplify building data pipelines for use cases including:
# MAGIC
# MAGIC * Auditing Data Changes
# MAGIC * Reproducing experiments & reports
# MAGIC * Rollbacks
# MAGIC
# MAGIC As you write into a Delta table or directory, every operation is automatically versioned.
# MAGIC
# MAGIC <img src="https://github.com/risan4841/img/blob/master/transactionallogs.png?raw=true" width=250/>
# MAGIC
# MAGIC You can query snapshots of your tables by:
# MAGIC 1. **Version number**, or
# MAGIC 2. **Timestamp.**
# MAGIC
# MAGIC using Python, Scala, and/or SQL syntax; for these examples we will use the SQL syntax.  
# MAGIC
# MAGIC For more information, refer to the [docs](https://docs.delta.io/latest/delta-utility.html#history), or [Introducing Delta Time Travel for Large Scale Data Lakes](https://databricks.com/blog/2019/02/04/introducing-delta-time-travel-for-large-scale-data-lakes.html)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --SQL Version
# MAGIC DESCRIBE HISTORY sensor_readings_historical_silver
# MAGIC --select max(version) from (DESCRIBE HISTORY sensor_readings_historical_silver)
# MAGIC
# MAGIC -- Python Version
# MAGIC /*
# MAGIC display(DeltaTable.forName(spark, "sensor_readings_historical_silver").history())
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ah, version 1 should have the 999.99 values
# MAGIC SELECT * FROM sensor_readings_historical_silver version AS OF 0 WHERE reading_1 = 999.99

# COMMAND ----------

spark.conf.set("spark.sql.ansi.enabled","true")	
df=spark.table("sensor_readings_historical_silver@v3")
display(df.where("reading_1=999.99"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##![Delta Lake Logo Tiny](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) Schema Evolution
# MAGIC With the `mergeSchema` option, you can evolve your Delta Lake table schema

# COMMAND ----------

from pyspark.sql.functions import *
dataPath = f"dbfs:/FileStore/flight/{team_name}/assignment_1_ingest.csv"

schemas=T.StructType([
  T.StructField("id", T.StringType(), True),
  T.StructField("reading_time", T.TimestampType(), True),
  T.StructField("device_type", T.StringType(), True),
  T.StructField("device_id", T.StringType(), True),
  T.StructField("device_operational_status", T.StringType(), True),
  T.StructField("reading_1", T.DoubleType(), True),
  T.StructField("reading_2", T.DoubleType(), True),
  T.StructField("reading_3", T.DoubleType(), True),
])

df = (spark
      .read
      .option("header", True)
      .schema(schemas)
      .csv(dataPath)
     )
df.write.format("delta").mode("overwrite").partitionBy("device_type").save("/FileStore/data")

df2 = spark.read.option("header", True).schema(schemas).csv(dataPath).withColumn("Lebara",col("reading_3")+1)

df2.write.format("delta").mode("append").option("mergeSchema",True).partitionBy("device_type").save("/FileStore/data")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <li>By default, updateAll and insertAll assign all the columns in the target Delta table with columns of the same name from the source dataset. Any columns in the source dataset that don’t match columns in the target table are ignored. However, in some use cases, it is desirable to automatically add source columns to the target Delta table. To automatically update the table schema during a merge operation with updateAll and insertAll (at least one of them), you can set the Spark session configuration spark.databricks.delta.schema.autoMerge.enabled to true before running the merge operation </li>

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# COMMAND ----------

from pyspark.sql.functions import *
dataPath = f"dbfs:/FileStore/flight/{team_name}/assignment_1_ingest.csv"

schemas=T.StructType([
  T.StructField("id", T.StringType(), True),
  T.StructField("reading_time", T.TimestampType(), True),
  T.StructField("device_type", T.StringType(), True),
  T.StructField("device_id", T.StringType(), True),
  T.StructField("device_operational_status", T.StringType(), True),
  T.StructField("reading_1", T.DoubleType(), True),
  T.StructField("reading_2", T.DoubleType(), True),
  T.StructField("reading_3", T.DoubleType(), True),
])

df = (spark
      .read
      .option("header", True)
      .schema(schemas)
      .csv(dataPath)
     )
df.write.format("delta").mode("overwrite").partitionBy("device_type").save("/FileStore/data")

df2 = spark.read.option("header", True).schema(schemas).csv(dataPath).withColumn("Lebara",col("reading_3")+1)
#df2 = spark.read.option("header", True).schema(schemas).csv(dataPath).drop("reading_3")
df2.write.format("delta").mode("append").save("/FileStore/data")


# COMMAND ----------

df2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partitioning
# MAGIC The current silver table is not partitioned with respect to a specific key/column.
# MAGIC - Might lead to slower queries for business analysts
# MAGIC Business analysts often look at specific devices and certain time periods. To speed up such queries we could partition by :
# MAGIC - device_id
# MAGIC - time -> date, hour, minute

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partition by device
# MAGIC There were only 3 distinct dates in our table so instead we choose to partition by device.
# MAGIC
# MAGIC - Partition silver table by device
# MAGIC - Demonstrate partitioning
# MAGIC - Explore corresponding directories using dbutils

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a Silver table partitioned by Device. 
# MAGIC -- Create a new table, so we can compare new and old
# MAGIC
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver_by_device;
# MAGIC
# MAGIC CREATE TABLE sensor_readings_historical_silver_by_device 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (device_id)
# MAGIC AS (SELECT * FROM sensor_readings_historical_silver)
# MAGIC
# MAGIC -- ALTER TABLE table_name ADD PARTITION part_spec 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can see partition information
# MAGIC
# MAGIC DESCRIBE EXTENDED sensor_readings_historical_silver_by_device

# COMMAND ----------

# MAGIC %md
# MAGIC #### Partition by date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Maybe Date would be a good way to partition the data
# MAGIC
# MAGIC SELECT DISTINCT DATE(reading_time) FROM sensor_readings_historical_silver
# MAGIC
# MAGIC -- Hmmm, there are only three dates, so maybe that's not the best choice.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compare query times for different partitionings
# MAGIC - No partition
# MAGIC - Parition by Device_id
# MAGIC - Partition by date, hour & minute
# MAGIC
# MAGIC The query is faster when using the un-partitioned table. One reason might be the overhead of many small files added when partitioning to fine-grained.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver_by_hour_and_minute;
# MAGIC
# MAGIC CREATE TABLE sensor_readings_historical_silver_by_hour_and_minute(
# MAGIC id string,
# MAGIC reading_time timestamp,
# MAGIC device_type string,
# MAGIC device_id string,
# MAGIC device_operational_status string,
# MAGIC reading_1 double,
# MAGIC reading_3 double,
# MAGIC date_reading_time DATE GENERATED ALWAYS AS (CAST(reading_time AS DATE)),
# MAGIC hour_reading_time int GENERATED ALWAYS AS (hour(reading_time)),
# MAGIC minute_reading_time int GENERATED ALWAYS AS (minute(reading_time)),
# MAGIC pk bigInt GENERATED ALWAYS as identity ( start with 0 increment by 1) 
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (date_reading_time, hour_reading_time, minute_reading_time)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC To insert into a table using generated columns, you need to either provide values for all columns, or use insert into t1(c1, c2, ...) without any generated columns in the list.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into sensor_readings_historical_silver_by_hour_and_minute (
# MAGIC id,
# MAGIC reading_time ,
# MAGIC device_type ,
# MAGIC device_id ,
# MAGIC device_operational_status ,
# MAGIC reading_1 ,
# MAGIC reading_3 ) Table sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct(pk)) from sensor_readings_historical_silver_by_hour_and_minute 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sensor_readings_historical_silver_by_hour_and_minute limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE 
# MAGIC   DATE(reading_time) = '2015-02-24' AND
# MAGIC   HOUR(reading_time) = '14' AND
# MAGIC   MINUTE(reading_time) = '2'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver_by_device
# MAGIC WHERE 
# MAGIC   DATE(reading_time) = '2015-02-24' AND
# MAGIC   HOUR(reading_time) = '14' AND
# MAGIC   MINUTE(reading_time) = '2'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC optimize sensor_readings_historical_silver_by_device zorder by reading_time

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver_by_hour_and_minute
# MAGIC WHERE 
# MAGIC   date_reading_time = '2015-02-24' AND
# MAGIC   hour_reading_time = '14' AND
# MAGIC   minute_reading_time = '2'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create gold tables

# COMMAND ----------

# MAGIC %md
# MAGIC Here we create Gold tables - aggregating data from our Silver tables, that we can immediately visualise here and serve to analysts through their preferred BI tools.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Here is an example of a Gold table
# MAGIC
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_gold;
# MAGIC
# MAGIC CREATE TABLE sensor_readings_historical_gold
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC    delta.dataSkippingNumIndexedCols = 8,
# MAGIC    delta.autoOptimize.optimizeWrite = true,
# MAGIC    delta.logRetentionDuration = "interval 35 days",
# MAGIC    delta.deletedFileRetentionDuration = "interval 10 days"
# MAGIC   )
# MAGIC AS
# MAGIC SELECT device_id, count(reading_time) AS count
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE device_operational_status LIKE "FAILURE"
# MAGIC GROUP BY device_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sensor_readings_historical_gold
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <H1> Vacuum </H1>
# MAGIC
# MAGIC Recursively vacuum directories associated with the Delta table and remove data files that are no longer in the latest state of the transaction log for the table and are older than a retention threshold. Files are deleted according to the time they have been logically removed from Delta’s transaction log + retention hours, not their modification timestamps on the storage system. The default threshold is 7 days.
# MAGIC By default you can time travel to a Delta table up to 30 days old unless you have:
# MAGIC
# MAGIC Changed the data or log file retention periods using the following table properties:<br>
# MAGIC delta.logRetentionDuration = "interval <interval>": controls how long the history for a table is kept. The default is interval 30 days.<br>
# MAGIC Each time a checkpoint is written, Delta automatically cleans up log entries older than the retention interval. If you set this config to a large enough value, many log entries are retained. This should not impact performance as operations against the log are constant time. Operations on history are parallel but will become more expensive as the log size increases.<br>
# MAGIC delta.deletedFileRetentionDuration = "interval <interval>": controls how long ago a file must have been deleted before being a candidate for VACUUM. The default is interval 7 days.<br>
# MAGIC To access 30 days of historical data even if you run VACUUM on the Delta table, set delta.deletedFileRetentionDuration = "interval 30 days". This setting may cause your storage costs to go up.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "False")

# COMMAND ----------

# MAGIC %sql
# MAGIC --VACUUM sensor_readings_historical_gold retain 120 hours dry run;
# MAGIC vacuum delta.`/FileStore/data`  retain 5040 hours dry run;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history   delta.`/FileStore/data` ;
# MAGIC
# MAGIC --show tblproperties delta.`/FileStore/data` 

# COMMAND ----------

# MAGIC %md
# MAGIC <h1> Shallow and Deep clone </h1>

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table Tableshallow shallow clone sensor_readings_historical_silver version as of 1  location "/FileStore/companyLebara";

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, input_file_name() from Tableshallow

# COMMAND ----------

dbutils.fs.ls("/FileStore/companyLebara")

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC update Tableshallow set device_id='9I009R' 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, input_file_name() from Tableshallow

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history Tableshallow

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table Tabledeep deep clone sensor_readings_historical_silver version as of 2
# MAGIC  location "/FileStore/companyLebaras";

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history Tabledeep

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *, input_file_name() from Tabledeep;
