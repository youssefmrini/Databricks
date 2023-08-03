-- Databricks notebook source
-- MAGIC %py 
-- MAGIC dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # Delta Lake SQL Rapidstart
-- MAGIC
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" style="float:right; width:300px; clear:both"/>
-- MAGIC
-- MAGIC ## Why Delta Lake?
-- MAGIC
-- MAGIC Running ingestion pipeline on Cloud Storage can be very challenging. Data teams are typically facing the following challenges:
-- MAGIC
-- MAGIC * Hard to append data *(Adding newly arrived data leads to incorrect reads)*
-- MAGIC * Modification of existing data is difficult (*GDPR/CCPA requires making fine grained changes to existing data lake)*
-- MAGIC * Jobs failing mid way (*Half of the data appears in the data lake, the rest is missing)*
-- MAGIC * Real-time operations (*Mixing streaming and batch leads to inconsistency)*
-- MAGIC * Costly to keep historical versions of the data (*Regulated environments require reproducibility, auditing, governance)*
-- MAGIC * Difficult to handle large metadata (*For large data lakes the metadata itself becomes difficult to manage)*
-- MAGIC * “Too many files” problems (*Data lakes are not great at handling millions of small files)*
-- MAGIC * Hard to get great performance (*Partitioning the data for performance is error-prone and difficult to change)*
-- MAGIC * Data quality issues (*It’s a constant headache to ensure that all the data is correct and high quality)*
-- MAGIC
-- MAGIC Theses challenges have a real impact on team efficiency and productivity, spending unecessary time fixing low-level, technical issues instead on focusing on the high-level, business implementation.
-- MAGIC
-- MAGIC ## What's Delta Lake?
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-lake-acid.png" style="width:400px; float: right; margin: 50px 0px 0px 30px"/>
-- MAGIC
-- MAGIC Delta Lake is a Storage Layer a top blob storage for Reliability and Streaming / Batch data pipelines. It brings the following capabilities:
-- MAGIC
-- MAGIC * ACID transactions
-- MAGIC * Support for DELETE/UPDATE/MERGE
-- MAGIC * Unify batch & streaming
-- MAGIC * Time Travel
-- MAGIC * Clone zero copy
-- MAGIC * Generated partitions
-- MAGIC * CDF - Change Data Flow (DBR runtime)
-- MAGIC * ...
-- MAGIC
-- MAGIC By solving these challenges, Delta Lakes accelerate your team productivity and allow Data Analyst team building simple and reliable pipelines.
-- MAGIC
-- MAGIC <br style="clear:both" />
-- MAGIC
-- MAGIC ## Rapid start Objectives
-- MAGIC
-- MAGIC This notebook will discuss and demonstrate many ways of interacting with data and the Delta Lake format using Spark SQL. You will learn how to query various sources of data, how to store that data in Delta, how to manage and interrogate metadata, how to refine data using typical SQL DML commands (MERGE, UPDATE, DELETE), and how to optimize and manage your Delta Lake. 
-- MAGIC
-- MAGIC By the end of this notebook, you will have performed the following activities and produced your very own Delta Lake:
-- MAGIC
-- MAGIC 1. Direct queried different data sources using Spark SQL
-- MAGIC 2. Create tables
-- MAGIC 3. Create, manipulate and explore metadata
-- MAGIC 4. Refine datasets using MERGE, UDPATE and DELETE
-- MAGIC 5. Track table changes with CDF
-- MAGIC 6. Explore historical versions of data
-- MAGIC 7. Combine and aggregate datasets to produce refinement layers
-- MAGIC 8. Schema enforcement
-- MAGIC 9. Optimize tables and manage the lake
-- MAGIC 10. Generated columns
-- MAGIC
-- MAGIC
-- MAGIC <!-- do not remove -->
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_lake%2Frapid_start&dt=RAPID_START_DELTA">
-- MAGIC <!-- [metadata={"description":"Full Delta Lake rapid start, covering all the features. <br/><i>Use this content for an extensive Delta Lake workshop.</i>",
-- MAGIC  "authors":["daniel.arrizza@databricks.com"],
-- MAGIC  "db_resources":{}}] -->

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC Run the below cell to set up our temporary database.

-- COMMAND ----------

-- MAGIC %run ./_resources/00-setup $reset_all_data=$reset_all_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Query our Lakehouse
-- MAGIC
-- MAGIC Databricks lets you query files stored in cloud storage like S3/ADLS/Google Storage.
-- MAGIC
-- MAGIC In this case, we'll be querying files stored in Databricks hosted storage.
-- MAGIC
-- MAGIC [These files are mounted to your Databricks File System under the /databricks-datasets/ path](https://docs.databricks.com/data/databricks-datasets.html), which makes them easier to reference from notebook code.
-- MAGIC
-- MAGIC You can find instructions on how to mount your own buckets [here](https://docs.databricks.com/data/databricks-file-system.html#mount-storage).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Magic Commands
-- MAGIC
-- MAGIC [Magic commands](https://docs.databricks.com/notebooks/notebooks-use.html#mix-languages) are shortcuts that allow for language switching in a notebook
-- MAGIC
-- MAGIC `%fs` is a shortcut for interacting with [dbutils files sytem utilities](https://docs.databricks.com/dev-tools/databricks-utils.html#file-system-utilities). In this case, we are listing out the files under our IOT streaming data directory.
-- MAGIC
-- MAGIC Notice that the data-device data is broken into many compressed JSON files. We will read these in by specifying their directory, not by specifying individual files.

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-user

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-device

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CSV Data
-- MAGIC
-- MAGIC CSV data can be direct queried this way, but if your data has headers or you'd like to specify options, you'll need to create a tempoary view instead.

-- COMMAND ----------

SELECT * FROM csv.`/databricks-datasets/iot-stream/data-user/userData.csv`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.option("delimiter",",").option("header","true").csv("/databricks-datasets/iot-stream/data-user/userData.csv")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As you can see, this particular dataset has headers, which means we'll need to create a temporary view in order to properly query this inforamtion. 
-- MAGIC
-- MAGIC Specifically, we'll need the temporary view in order to specify the header and the schema. Specifying the schema can improve performance by preventing an initial inference scan. However, because this dataset is small, we'll infer the schema.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW userData
USING csv
OPTIONS(
  path '/databricks-datasets/iot-stream/data-user/userData.csv',
  header true,
  inferSchema true
);

SELECT * FROM userData;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC userData=spark.read.format("csv").option("header","True").option("delimiter",",").load("/databricks-datasets/iot-stream/data-user/userData.csv")
-- MAGIC userData.createOrReplaceTempView("userData")
-- MAGIC display(spark.sql("select * from userData"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JSON Data
-- MAGIC
-- MAGIC JSON files can also be directly queried much like CSVs. Spark can also read in a full directory of JSON files and allow you to directly query them. We'll use a similar syntax to the CSV direct read, but we'll specify 'JSON' instead for this read. 

-- COMMAND ----------

SELECT * FROM JSON.`/databricks-datasets/iot-stream/data-device` 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC device=spark.read.format("json").load("/databricks-datasets/iot-stream/data-device")
-- MAGIC display(device)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Creating the Delta Tables
-- MAGIC
-- MAGIC We'll read the CSV and JSON table to create Delta Lake table.
-- MAGIC
-- MAGIC Delta tables live in cloud storage. In addition, they can be registered with the metastore and made available to other users.
-- MAGIC
-- MAGIC New databases and tables that are registered with the metastore can be found in the Data tab on the left. 
-- MAGIC
-- MAGIC There are two types of tables that can be created: [managed and unmanaged](https://docs.databricks.com/data/tables.html#managed-and-unmanaged-tables)
-- MAGIC - Managed tables are stored in the Metastore and are enitrely managed by it. Dropping a managed table will delete the underlying files and data.
-- MAGIC - Unmanaged tables are pointers to files and data stored in other locations. Dropping an unmanaged table will delete the metadata and remove the referencable alias for the dataset. However, the underlying data and files will remain undeleted in their orginal location. 
-- MAGIC
-- MAGIC This rapid start will be creating managed tables. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using Autoloader and Python with CSV User Table
-- MAGIC
-- MAGIC Let's create our first Delta table! Our initial table will incrementally consume all the `user_data_raw_csv` files and append them to the Bronze Delta Table!
-- MAGIC
-- MAGIC We'll be using Databricks Autoloader.
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/auto-loader.png" width="1000" />
-- MAGIC
-- MAGIC Note that we don't have to create the table first in SQL, the engine will automatically get the existing `user_data_bronze` and add new data to it, or create the delta table on the current Database if it doesn't exists.

-- COMMAND ----------

-- DBTITLE 1,Use Autoloader and python to load data into our Delta "user_data_bronze" Table
-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC         .format("cloudFiles")
-- MAGIC         .option("cloudFiles.format", "csv")
-- MAGIC         .option("cloudFiles.schemaLocation", f"{cloud_storage_path}/delta/schema/data_device_csv")
-- MAGIC         .option("cloudFiles.inferColumnTypes", "true")
-- MAGIC         .option("header", "true")
-- MAGIC         .load("/databricks-datasets/iot-stream/data-user")
-- MAGIC       .writeStream
-- MAGIC          .option("checkpointLocation", f"{cloud_storage_path}/delta/checkpoint/user_data_bronze")
-- MAGIC          .trigger(once = True)
-- MAGIC          .table("user_data_bronze").awaitTermination())
-- MAGIC
-- MAGIC #Note that Databricks use the Delta format by default. No need for .format("delta") in the write path.

-- COMMAND ----------

-- MAGIC %md Note: want a nice, visual way to create your entire table pipeline with the autoloader without having to worry about checkpoints and other advanced options? Check out Delta Live Tables!

-- COMMAND ----------

SELECT * FROM user_data_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using COPY INTO and SQL to incrementally load the Device Table
-- MAGIC
-- MAGIC For this example, we'll be using pure SQL. The first thing to do is to create our bronze Delta table using plain SQL command.
-- MAGIC
-- MAGIC Once it's created, we can use the `COPY INTO` SQL statement to incrementally load new data to our bronze table. Not that if this command is re-run multiple time we won't be loading the data twice.

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot-stream/data-device

-- COMMAND ----------

-- DBTITLE 1,Create the Device Bronze Delta table
CREATE TABLE IF NOT EXISTS device_data_bronze(
  calories_burnt DOUBLE,
  device_id BIGINT,
  id BIGINT,
  miles_walked BIGINT,
  num_steps BIGINT,
  `timestamp` STRING,
  user_id BIGINT,
  value STRING) COMMENT "Device Data Table - No Transformations";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from  delta import *
-- MAGIC DeltaTable.createIfNotExists(spark) \
-- MAGIC   .tableName("device_data_bronze") \
-- MAGIC   .addColumn("calories_burnt", "DOUBLE") \
-- MAGIC   .addColumn("device_id", "BIGINT") \
-- MAGIC   .addColumn("id", "BIGINT") \
-- MAGIC   .addColumn("miles_walked", "BIGINT") \
-- MAGIC   .addColumn("num_steps", "STRING") \
-- MAGIC   .addColumn("`timestamp`", "STRING") \
-- MAGIC   .addColumn("user_id", "BIGINT") \
-- MAGIC   .addColumn("value", "STRING") \
-- MAGIC   .comment("Device Data Table - No Transformations") \
-- MAGIC   .execute()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC table=DeltaTable.forName(spark,"device_data_bronze")
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Incremental Data Loading in Delta with COPY INTO
COPY INTO device_data_bronze FROM '/databricks-datasets/iot-stream/data-device' FILEFORMAT = JSON

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Describe the Delta Tables
-- MAGIC Congrats, our two Delta Tables have been created! Feel free to use the python Streaming API or the SQL one to load your data.
-- MAGIC
-- MAGIC We can use the `DESCIBE EXTENDED device_data_bronze` syntax to review the table definition (or `SHOW CREATE TABLE device_data_bronze`).

-- COMMAND ----------

DESCRIBE EXTENDED device_data_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Add Metadata
-- MAGIC
-- MAGIC Comments can be added by column or at the table level. Comments are useful metadata that can help other users better understand data definitions or any caveats about using a particular field.
-- MAGIC
-- MAGIC To add a comment to table or column use the `ALTER TABLE` syntax. [Comments can also be added to tables, columns and partitions when they are created](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-hiveformat.html#create-table-with-hive-format) (see example above).
-- MAGIC
-- MAGIC Comments will also appear in the UI to further help other users understand the table and columms:

-- COMMAND ----------

ALTER TABLE user_data_bronze ALTER COLUMN bp COMMENT 'Can be High or Normal';
ALTER TABLE user_data_bronze ALTER COLUMN smoker COMMENT 'Can be Y or N';
ALTER TABLE user_data_bronze ALTER COLUMN familyhistory COMMENT 'Can be Y or N';
ALTER TABLE user_data_bronze ALTER COLUMN cholestlevs COMMENT 'Can be High or Normal';
ALTER TABLE user_data_bronze SET TBLPROPERTIES ('comment' = 'User Data Raw Table describing risk levels based on health factors');

-- COMMAND ----------

-- DBTITLE 1,Describe table definition
-- this shows the column schema and comments
DESCRIBE TABLE EXTENDED user_data_bronze


-- COMMAND ----------

-- DBTITLE 1,Describe table Details with Delta information
-- this one has number of files and sizeInBytes
DESCRIBE DETAIL user_data_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Enforce Data Quality with constraint
-- MAGIC
-- MAGIC Delta Lake support constraints. You can add any expression to force your table having a given field respecting this constraint. As example, let's make sure that the ID is never null, and the age between 0 and 200.
-- MAGIC
-- MAGIC *Note: This is enforcing quality at the table level. Delta Live Tables offer much more advance quality rules and expectations in data Pipelines.*

-- COMMAND ----------

ALTER TABLE user_data_bronze ADD CONSTRAINT userid_not_null CHECK (userid IS NOT null);
ALTER TABLE user_data_bronze ADD CONSTRAINT age_valid CHECK (age > 0 and age < 200);

-- COMMAND ----------

-- DBTITLE 1,Inserting data with NULL value will fail as expected:
-- uncomment the next line to try inserting data with incorrect userid
-- INSERT INTO user_data_bronze (userid, gender, age, height, weight, smoker, familyhistory, cholestlevs, bp, risk, _rescued_data) 
--   VALUES (null, 'F', 40, 54, 100, 'N', 'Y', 'High', 'High', 5, null);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Show Table in Data Tab
-- MAGIC
-- MAGIC Click on the Data tab.
-- MAGIC <img src="https://docs.databricks.com/_images/data-icon.png" /></a>
-- MAGIC
-- MAGIC You can see your table under `features_<username>.user_data_bronze` in the data tab
-- MAGIC
-- MAGIC
-- MAGIC <img src="https://docs.databricks.com/_images/default-database.png"/></a>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DELETE, UPDATE, UPSERT (DML queries)
-- MAGIC
-- MAGIC Parquet has traditionally been the primary data storage layer in data lakes. Parquet has some substantial drawbacks.
-- MAGIC
-- MAGIC Parquet cannot support full SQL DML as Parquet is immuatable and append only. In order to delete a row the whole file must be recreated.
-- MAGIC
-- MAGIC Delta manages this process and allows for full use of SQL DML on a data lake.

-- COMMAND ----------

-- before updating our table, let's turn on CDC. Don't worry about this setup for now, it's describe in detail the next section.
ALTER TABLE user_data_bronze SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DELETE
-- MAGIC
-- MAGIC Delta allows for SQL DML deletes, which can allow Delta to support GDPR and CCPA use cases. 
-- MAGIC
-- MAGIC We have two 25 year olds in or dataset and we have just recieved a right to be forgotten request from user 21

-- COMMAND ----------

SELECT * FROM user_data_bronze WHERE age = 25

-- COMMAND ----------

DELETE FROM user_data_bronze where age = 25;
SELECT * FROM user_data_bronze WHERE age = 25

--%python
--table=DeltaTable.forName(spark,"device_data_bronze")
--table.delete("user_id = 21")
--tabledelta=spark.table("device_data_bronze")
--display(tabledelta.where("user_id = 21"))



-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### UPDATE
-- MAGIC
-- MAGIC Delta also allows for rows to be updated in place using simple SQL DML.

-- COMMAND ----------

SELECT * FROM user_data_bronze WHERE userid = 1

-- COMMAND ----------

-- MAGIC %md We have recieved updated information that user 1's chosesterol levels have droped to normal levels

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import when
-- MAGIC df=spark.table("user_data_bronze")
-- MAGIC
-- MAGIC dfedited=df.withColumn("cholestlevs",when(df["userid"]==1,"normal").otherwise(df["cholestlevs"]))
-- MAGIC dfedited.write.mode("overwrite").option("overwriteSchema","true").saveAsTable("user_data_bronze")
-- MAGIC
-- MAGIC
-- MAGIC #%sql
-- MAGIC #UPDATE user_data_bronze SET cholestlevs = 'Normal' WHERE userid = 1;
-- MAGIC #SELECT * FROM user_data_bronze where userid = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UPSERT / MERGE
-- MAGIC
-- MAGIC Delta allows for simple upserts using a Merge syntax.
-- MAGIC
-- MAGIC Merge let you Upsert a given dataframe based on a specific key (typically id):
-- MAGIC
-- MAGIC * If the ID matches, we want to UPDATE the row with the given values. 
-- MAGIC * If the ID match and the row is flagged for deletion, we want to DELETE the existing row
-- MAGIC * If it doesn't  match an existing ID we want to APPEND the row to the table
-- MAGIC
-- MAGIC Note the initial set of users of age 34 in our data below.

-- COMMAND ----------

SELECT * FROM user_data_bronze WHERE age = 34
--%python
--information=spark.table("user_data_bronze").where("age=34")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's create the new data, which we will merge into our original data set. 
-- MAGIC
-- MAGIC * User 2 has information that will be updated (They have lost 10 pounds) 
-- MAGIC * A new user (userid = 39), who is 34 too and was not previously in the data set, will be added. 
-- MAGIC * User 15 will be removed (mark as DELETED)

-- COMMAND ----------

-- create the upsert table
CREATE TABLE IF NOT EXISTS user_data_to_upsert 
  (userid INT, gender STRING, age INT, height INT, weight INT, smoker STRING, familyhistory STRING, cholestlevs STRING, bp STRING, risk INT, deleted boolean, _rescued_data STRING);

-- clean up the table to avoid duplicate
DELETE from user_data_to_upsert; 

-- Insert our set of user to UPSERT
INSERT INTO user_data_to_upsert VALUES 
  (2,  "M", 34 , 69, 140, "N", "Y", "Normal", "Normal", -10, false, null), 
  (39,  "M", 34 , 72, 155, "N", "Y", "Normal", "Normal", 10, false, null), 
  (15, null, null , null, null, null, null, null, null, null, true, null); -- user to delete (id 15 flagged as `deleted`) 
  
select * from user_data_to_upsert;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's do the merge (upsert) from `user_data_upsert_values` into `user_data_bronze_delta`
-- MAGIC
-- MAGIC Note that user 2 now has a weight of 140 and we have a new user 39. User 15 has been removed!

-- COMMAND ----------

MERGE INTO user_data_bronze as target
USING user_data_to_upsert as source
ON target.userid = source.userid
WHEN MATCHED AND source.deleted THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED AND not source.deleted
  THEN INSERT *;
  
SELECT * FROM user_data_bronze WHERE age = 34;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC # CDF: Consuming Change Data Feed from Delta tables
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-cdf-datamesh.png" style="float:right; margin-right: 50px" width="300px" />
-- MAGIC
-- MAGIC We have updated, deleted, inserted data into the `user_data_bronze` table. It's often useful to be able to consume theses changes (query which row has been updated or deleted in the table).
-- MAGIC
-- MAGIC This can be done using Databricks Change Data Feed (CDF) feature. To do so, we need to make sure the CDF are enabled at the table level. Once enabled, it'll capture all the table modifications using the `table_changes` function.
-- MAGIC
-- MAGIC For more details, visit the [CDF documentation](https://docs.databricks.com/delta/delta-change-data-feed.html)
-- MAGIC
-- MAGIC CDF makes **Data Mesh** implementation easier. Once enabled by an organisation, data can be shared with other. It's then easy to subscribe to the modification stream and propagage GDPR DELETE downstream.

-- COMMAND ----------

-- DBTITLE 1,Enable CDF at the table level
ALTER TABLE user_data_bronze SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- COMMAND ----------

-- MAGIC %md #### Delta CDF table_changes output
-- MAGIC In addition to the row details, `table_changes` provides back 4 cdc types in the "_change_type" column:
-- MAGIC
-- MAGIC | CDC Type             | Description                                                               |
-- MAGIC |----------------------|---------------------------------------------------------------------------|
-- MAGIC | **update_preimage**  | Content of the row before an update                                       |
-- MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
-- MAGIC | **delete**           | Content of a row that has been deleted                                    |
-- MAGIC | **insert**           | Content of a new row that has been inserted                               |
-- MAGIC
-- MAGIC Let's query the changes of the Delta Version 12 which should be our MERGE operation (you can run a `DESCRIBE HISTORY user_data_bronze` to see the version numbers).
-- MAGIC
-- MAGIC As you can see 1 row has been UPDATED (we get the old and new value), 1 DELETED and one INSERTED.

-- COMMAND ----------

SELECT * FROM table_changes("user_data_bronze", 12);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # History
-- MAGIC
-- MAGIC Delta keeps track of all previous commits to the table. We can see that history, query previous states, and rollback.

-- COMMAND ----------

DESCRIBE HISTORY user_data_bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We query the latest version by default

-- COMMAND ----------

SELECT * FROM user_data_bronze WHERE age = 34

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now, let's try querying with the `VERSION AS OF` command. We can see the data we previously deleted. There is also `TIMESTAMP AS OF`:
-- MAGIC ```
-- MAGIC SELECT * FROM table_identifier TIMESTAMP AS OF timestamp_expression
-- MAGIC SELECT * FROM table_identifier VERSION AS OF version
-- MAGIC ```
-- MAGIC
-- MAGIC Let's query the table before our MERGE operation (ex: version 10). As you can see, ID 39 doesn't exist and we get the old values (ex: userid 2 weight is 150 vs 140)

-- COMMAND ----------

SELECT * FROM user_data_bronze
    VERSION AS OF 10
  WHERE age = 34

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Restore a Previous Version
-- MAGIC You can restore a Delta table to its earlier state by using the `RESTORE` command
-- MAGIC
-- MAGIC ⚠️ Databricks Runtime 7.4 and above

-- COMMAND ----------

RESTORE TABLE user_data_bronze TO VERSION AS OF 10;

SELECT *
  FROM user_data_bronze
WHERE age = 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CLONE
-- MAGIC You can create a copy of an existing Delta table at a specific version using the `clone` command. There are two types of clones:
-- MAGIC * A **deep clone** is a clone that copies the source table data to the clone target in addition to the metadata of the existing table. 
-- MAGIC * A **shallow clone** is a clone that does not copy the data files to the clone target. The table metadata is equivalent to the source. These clones are cheaper to create.
-- MAGIC
-- MAGIC Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.
-- MAGIC
-- MAGIC This is very useful to get data from a PROD environment to a STAGING one, or archive a specific version for regulatory reason.
-- MAGIC
-- MAGIC *Note: Shallow clone are pointers to the main table. Running a VACUUM may delete the underlying files and break the shallow clone!*

-- COMMAND ----------

-- DBTITLE 1,Shallow clone (zero copy)
CREATE TABLE IF NOT EXISTS user_data_bronze_clone
  SHALLOW CLONE user_data_bronze
  VERSION AS OF 3;

SELECT * FROM user_data_bronze_clone;

-- COMMAND ----------

-- DBTITLE 1,Deep clone (copy data)
CREATE TABLE IF NOT EXISTS user_data_bronze_clone_deep
  DEEP CLONE user_data_bronze;

SELECT * FROM user_data_bronze_clone_deep

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Medallion Architecture

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze, Silver and Gold Tables
-- MAGIC
-- MAGIC One of the best practice building Data Pipeline is to create multiple layer of tables, increasing data quality from Bronze to Gold.
-- MAGIC
-- MAGIC The number of layer is entirely dependent of your workload. Here is a typical example:
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-pipeline.png" width="800px" />
-- MAGIC
-- MAGIC
-- MAGIC **Bronze Data**
-- MAGIC * Data consumed from file, using the autoloader, without transformation. Incorrect data can always be retrieved using the `rescued_data` column.
-- MAGIC
-- MAGIC **Silver Data**
-- MAGIC * Bronze data being cleaned. Timestamps are properly formated, data is ready to be queried by Data Analyst.
-- MAGIC
-- MAGIC **Gold Data**
-- MAGIC * Silver data being join/enriched with another table, or aggregated to answer a specific business need (potentially adding extra information from a ML model).

-- COMMAND ----------

-- MAGIC %md ### Medallion Architecture with Delta Live Table (DLT)
-- MAGIC
-- MAGIC While Medallon architecture are typically developped in notebooks using the Python streaming API, [Delta Live Table](https://databricks.com/fr/product/delta-live-tables) greatly simplify data pipeline development, accelerating team efficiency and letting Data Analyst build robust pipeline without strong Data Engineer background.
-- MAGIC
-- MAGIC <img width="800" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/retail-dlt-pipeline.png"/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Schema Enforcement & Evolution
-- MAGIC **Schema enforcement**, also known as schema validation, is a safeguard in Delta Lake that ensures data quality.  Delta Lake uses schema validation *on write*, which means that all new writes to a table are checked for compatibility with the target table’s schema at write time. If the schema is not compatible, Delta Lake cancels the transaction altogether (no data is written), and raises an exception to let the user know about the mismatch.
-- MAGIC
-- MAGIC **Schema evolution** is a feature that allows users to easily change a table’s current schema to accommodate data that is changing over time. Most commonly, it’s used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.
-- MAGIC
-- MAGIC ### Schema Enforcement
-- MAGIC To determine whether a write to a table is compatible, Delta Lake uses the following rules. The DataFrame to be written:
-- MAGIC * Cannot contain any additional columns that are not present in the target table’s schema. 
-- MAGIC * Cannot have column data types that differ from the column data types in the target table.
-- MAGIC * Can not contain column names that differ only by case.

-- COMMAND ----------

-- You can uncomment the next line to see the error (remove the -- at the beginning of the line)
-- INSERT INTO TABLE user_data_bronze VALUES ('this is a test')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema Evolution
-- MAGIC
-- MAGIC Delta Lake support schema evolution. You can add a new column and even more advanced operation such as [updating partition](https://docs.databricks.com/delta/delta-batch.html#change-column-type-or-name)
-- MAGIC
-- MAGIC Databricks Autoloader support schema evolution by merging the schema with the new columns using the `.option("mergeSchema", "true")` option.
-- MAGIC
-- MAGIC More details from the [documentation](https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-alter-table.html)

-- COMMAND ----------

ALTER TABLE user_data_bronze ADD COLUMN test STRING ;

SELECT * FROM user_data_bronze;

-- COMMAND ----------

-- DBTITLE 1,Schema merge during MERGE uperation
-- Set this configuration to allow for schema evolution (ex: add new columns)
SET spark.databricks.delta.schema.autoMerge.enabled = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Optimize & Z-Ordering
-- MAGIC To improve query speed, Delta Lake on Databricks supports the ability to optimize the layout of data stored in cloud storage. 
-- MAGIC
-- MAGIC The `OPTIMIZE` command can be used to coalesce small files into larger ones.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Optimize & VACUUM
-- MAGIC
-- MAGIC Because of our multiple write / update / merge operation, our `user_data_bronze` tables has multiple files.
-- MAGIC Let's use `OPTIMIZE` to compact all theses files.
-- MAGIC
-- MAGIC Once `OPTIMIZE` is executed, we'll still keep the previous small files (they're still used for the time travel if you request a previous version of the table).
-- MAGIC
-- MAGIC The `VACUUM` command will cleanup all the previous files. By default, `VACUUM` keeps a couple of days. We'll force it to delete all the previous small files (preventing us to travel back in time). 
-- MAGIC
-- MAGIC It's best practice to periodically run OPTIMIZE and VACUUM on your delta table (ex: every week as a job)

-- COMMAND ----------

-- DBTITLE 1,Exploring `user_data_bronze` file layout on our blob storage
-- MAGIC %python
-- MAGIC #let's list all the files where our table has been physically stored. As you can see we have multiple small files
-- MAGIC display(dbutils.fs.ls(f'{cloud_storage_path}/tables/user_data_bronze'))

-- COMMAND ----------

-- Compact all small files
OPTIMIZE user_data_bronze;
-- allow Delta to drop with 0 hours (security added to avoid conflict, you wouldn't drop all the history in prod)
set spark.databricks.delta.retentionDurationCheck.enabled = false;
-- delete previous versions & small files
VACUUM user_data_bronze RETAIN 0 hours; 


-- COMMAND ----------

-- DBTITLE 1,List the file after our compaction
-- MAGIC %python
-- MAGIC #A single file remains!
-- MAGIC display(dbutils.fs.ls(f'{cloud_storage_path}/tables/user_data_bronze'))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## When to use ZORDER or Partitions
-- MAGIC
-- MAGIC ### ZORDER
-- MAGIC
-- MAGIC Databricks [ZORDER](https://databricks.com/fr/blog/2018/07/31/processing-petabytes-of-data-in-seconds-with-databricks-delta.html) is similar to adding indexes on a subset of columns. it can be applied on columns having high cardinality (that is, a large number of distinct values such as a UUID, or timestamp).
-- MAGIC
-- MAGIC Is a technique to colocate related information in the same set of files to improve query performance by reducing the amount of data that needs to be read.  If you expect a column to be commonly used in query predicates , then use `ZORDER BY`.
-- MAGIC
-- MAGIC Keep in mind that the more ZORDER column you have, the less gain you'll get from it. We typically recommend to have a max number of 3 - 4 columns in the ZORDER clause.
-- MAGIC
-- MAGIC ### Adding Partitions
-- MAGIC
-- MAGIC Partitions on the other hands require to have a much lower cardinality. Partitioning on UUID will create thousands of folders creating thousands of small files, drastically impacting your read performances.
-- MAGIC Your partition size should have more than 1GB of data. As example, a good partition could be the YEAR computed from a timestamp column.
-- MAGIC
-- MAGIC When ensure, prefer ZORDER over partitions as you'll never fall into small files issue with ZORDER (when you don't have partition)
-- MAGIC
-- MAGIC *Note: Zorder and partition can work together, ex: partition on the YEAR, ZORDER on UUID.*

-- COMMAND ----------

-- DBTITLE 1,Adding a ZORDER on multiple column
OPTIMIZE device_data_bronze ZORDER BY num_steps, calories_burnt

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that the metrics show that we removed more files than we added. 
-- MAGIC
-- MAGIC That's all we have to do, our table will now have very fast response time if you send a request by nm_steps or calories_burnt!

-- COMMAND ----------

SELECT * FROM device_data_bronze where num_steps > 7000 AND calories_burnt > 500

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC ## Blazing fast query at scale
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-lake-perf-bench.png" width="500" style="float: right; margin-left: 50px"/>
-- MAGIC
-- MAGIC
-- MAGIC Log files are compacted in a parquet checkpoint every 10 commits. The checkpoint file contains the entire table structure.
-- MAGIC
-- MAGIC Table is self suficient, the metastore doesn't store additional information removing bottleneck and scaling metadata.
-- MAGIC
-- MAGIC This result in **fast read query**, even with a growing number of files/partitions! 
-- MAGIC
-- MAGIC Adding ZORDER capabilities on top of that will provide fast response time for needle in the hay search! 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Auto-Optimize
-- MAGIC Auto Optimize is an optional set of features that automatically compact small files during individual writes to a Delta table. Paying a small cost during writes offers significant benefits for tables that are queried actively. Auto Optimize consists of two complementary features: **Optimized Writes** and **Auto Compaction**. More information on Auto Optimze can be found [here](https://docs.microsoft.com/en-us/azure/databricks/delta/optimizations/auto-optimize).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update Existing Tables

-- COMMAND ----------

ALTER TABLE user_data_bronze
SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL Configuration
-- MAGIC To ensure all new Delta tables have these features enabled, set the SQL configuration

-- COMMAND ----------

SET spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
SET spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Generated columns for dynamic partitions
-- MAGIC
-- MAGIC As discussed, a common pattern is to generate partition derivated from another column.
-- MAGIC
-- MAGIC Delta let you do that easily with Dynamic partition. Delta will generate columns based on expression and push-down the query filter to this partitioneven if the request is on the original field. 
-- MAGIC
-- MAGIC Let's see how this can be done using a timestamp column.

-- COMMAND ----------

select * from device_data_bronze

-- COMMAND ----------

CREATE TABLE if not exists device_data_bronze_partition (
  calories_burnt DOUBLE, 
  device_id BIGINT, 
  id BIGINT, 
  miles_walked DOUBLE, 
  num_steps BIGINT, 
  user_id BIGINT, 
  value STRING,
  timestamp STRING, 
  timestamp_date DATE GENERATED ALWAYS AS ( CAST(timestamp AS DATE) )
) PARTITIONED BY (timestamp_date) ;

-- COMMAND ----------

-- DBTITLE 1,Fill the table. Note that we don't have to specify the generated column
INSERT INTO device_data_bronze_partition (calories_burnt, device_id, id, miles_walked, num_steps, user_id, value, timestamp) 
    (SELECT calories_burnt, device_id, id, miles_walked, num_steps, user_id, value, timestamp FROM device_data_bronze);

-- COMMAND ----------

-- DBTITLE 1,A query on the timestamp will leverage the partition on our Generated Column
select * from device_data_bronze_partition where timestamp > '2018-07-21 01:18:10.732306'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Auto incrementing column with Identity Column
-- MAGIC
-- MAGIC Delta on Databricks let you create IDENTITY columns that will be automatically incremented by the engine.
-- MAGIC
-- MAGIC This is very powerful to support unique column for primary key. 
-- MAGIC
-- MAGIC Let's ses how this can be done by adding an extra column in our delta table.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS device_data_bronze_identity (
  auto_increment_id BIGINT GENERATED ALWAYS AS IDENTITY ( START WITH 10000 INCREMENT BY 1 ),
  calories_burnt DOUBLE, 
  device_id BIGINT, 
  id BIGINT, 
  miles_walked DOUBLE, 
  num_steps BIGINT, 
  user_id BIGINT, 
  value STRING,
  timestamp STRING);
  
INSERT INTO device_data_bronze_identity (calories_burnt, device_id, id, miles_walked, num_steps, user_id, value, timestamp) 
  (SELECT calories_burnt, device_id, id, miles_walked, num_steps, user_id, value, timestamp FROM device_data_bronze);

-- let's see the created auto increment id column:
SELECT * FROM device_data_bronze_identity;

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Delta Sharing
-- MAGIC
-- MAGIC Did you know? Delta tables can easily be shared with other organisation, being on Databricks or not, regardless of the cloud. For more details, have a look at [Delta Sharing OSS documentation](https://delta.io/sharing/)

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
