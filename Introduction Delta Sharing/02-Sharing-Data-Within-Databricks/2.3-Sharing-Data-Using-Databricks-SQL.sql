-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Sharing Data using Databricks SQL
-- MAGIC 
-- MAGIC 
-- MAGIC Databricks-managed Delta Sharing allows data providers to share data and data recipients to access the shared data. In this course, we are going to share data within Databricks, which means both data provider and recipient will be Databricks users. 
-- MAGIC 
-- MAGIC Databricks-managed Delta Sharing allows administrators to create and manage providers, shares, and recipients with a simple-to-use UI and SQL commands.
-- MAGIC 
-- MAGIC **In this demo, we will show data sharing process using SQL commands.** 
-- MAGIC 
-- MAGIC **Learning Objectives**
-- MAGIC - Create and manage shares using Databricks SQL queries
-- MAGIC - Create and manage recipients using Databricks SQL queries
-- MAGIC - Share data with change feed using Databricks SQL queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Prerequisites
-- MAGIC 
-- MAGIC In this lesson, we are going to share data using Delta Sharing. As Delta Sharing is integrated with Unity Catalog (UC), there are couple prerequisites that need you need to ensure before starting this course.
-- MAGIC 
-- MAGIC - Unity Catalog must be enabled, and at least one metastore must exist.
-- MAGIC - Delta sharing must be enabled for the metastore that you want to use for this course.
-- MAGIC - Only a metastore admin or account admin can share data using Delta Sharing. Therefore, make sure that you have necessary permissions to share data.
-- MAGIC - We need two accounts for this demo. One account will be used for sharing data and the second one will be used for accessing data.
-- MAGIC - The minimum DBR version required is **DBR 11.2**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Classroom Setup
-- MAGIC 
-- MAGIC 
-- MAGIC The first thing we're going to do is to run a setup script. This script will define the required configuration variables that are scoped to each user.
-- MAGIC 
-- MAGIC This script will create a catalog and a schema and import a sample dataset into the schema.  

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2.3

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Dataset Details
-- MAGIC 
-- MAGIC Run code block below to view necessary details that we will need in this course. Note the catalog name, database name and table for the sample data that we are going to share using Delta Sharing.
-- MAGIC 
-- MAGIC A sample dataset of flights data is created for you. **The table is partioned by `origin` field and Change Data Feed(CDF) is specified which will allow the recipient to query table data by version, starting from the current table version.**
-- MAGIC 
-- MAGIC In addition, as you progress through this course, you will see various references to the object **`DA`**. This object is provided by Databricks Academy and is part of the curriculum and not part of a Spark or Databricsk API. For example, the **`DA`** object exposes useful variables such as your username and various paths to the datasets in this course as seen here bellow. In this course we are going to use UI mostly, therefore, we are not going to need them for now.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"Username:          {DA.username}")
-- MAGIC print(f"Working Directory: {DA.paths.working_dir}")
-- MAGIC print(f"Catalog Name:      {DA.catalog_name}")
-- MAGIC print(f"Schema Name:       {DA.database_name}")
-- MAGIC print(f"Table Name:        {DA.table_name}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create & Manage Shares
-- MAGIC 
-- MAGIC The first of data sharing process is to create a share. In Delta Sharing, a share is a named object that contains a collection of tables in a metastore that you wish to share as a group. A share can contain tables from only a single metastore. You can add or remove tables from a share at any time.
-- MAGIC 
-- MAGIC 
-- MAGIC Following commands can help us to create and manage share.
-- MAGIC 
-- MAGIC - **`CREATE SHARE <share_name>`**: Creates a share.
-- MAGIC 
-- MAGIC - **`SHOW SHARES`** : Lists all shares. This command can be used with `LIKE` statement to filter the results of the statement.
-- MAGIC 
-- MAGIC - **`SHOW ALL IN SHARE <share_name>`**: Lists all tables in a share.
-- MAGIC 
-- MAGIC - **`ALTER SHARE`**: This command is used for renaming a share, editing share metadata and adding/removing tables in a share.
-- MAGIC 
-- MAGIC - **`DROP SHARE <share_name>`**: Deletes a share.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Create A Share
-- MAGIC 
-- MAGIC The first of sharing data with Delta Sharing is creating a share. In Delta Sharing, a share is a named object that contains a collection of tables in a metastore that you wish to share as a group. A share can contain tables from only a single metastore. You can add or remove tables from a share at any time.

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS flights_data_share 
COMMENT 'Delta Sharing share for flights data.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Manage Shares
-- MAGIC 
-- MAGIC Before adding tables to the share, let's review current shares. 

-- COMMAND ----------

-- Let's view all shares. This command should list all shares that you have access. 

SHOW SHARES;

-- COMMAND ----------

-- Filter shares by keyword. You should see the share we created in the previous step.

SHOW SHARES LIKE 'flights_data_share';

-- COMMAND ----------

-- We can all use DESCRIBE command to view details of a specific share

DESCRIBE SHARE flights_data_share;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Add Tables to the Share 
-- MAGIC 
-- MAGIC As next step fo data sharing, we need to add the table to the share. A share can multiple tables.

-- COMMAND ----------

-- Add flights table to the share

ALTER SHARE flights_data_share
  ADD TABLE db_flights.flights;

-- COMMAND ----------

-- We can remove rename tables in a share as;
-- ALTER SHARE flights_data_share REMOVE TABLE db_flights.flights;

-- COMMAND ----------

-- Check the tables in the share. This should show details of the table we just added to the share.

SHOW ALL IN SHARE flights_data_share;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create & Manage Recipients
-- MAGIC 
-- MAGIC A recipient represents an organization with whom to share data. Grant the recipient access to shares after they are created.
-- MAGIC 
-- MAGIC Following commands can be used to create and manage recipients;
-- MAGIC 
-- MAGIC - **`CREATE RECIPIENT`**: Create a recipient with `DATABRICKS` type or `TOKEN` type.
-- MAGIC 
-- MAGIC - **`SHOW RECIPIENTS`** : Lists all recipients. This command can be used with `LIKE` statement to filter the results of the statement.
-- MAGIC 
-- MAGIC - **`DESCRIBE RECIPIENT <recipient_name>`**: Returns the metadata of an existing recipient. The metadata information includes recipient name, and activation link (for TOKEN types). 
-- MAGIC 
-- MAGIC - **`ALTER RECIPIENT ...`**: This command is used for renaming a recipient and modifing the owner of the recipient.
-- MAGIC 
-- MAGIC - **`DROP RECIPIENT <recipient_name>`**: Deletes a recipient.
-- MAGIC 
-- MAGIC - **`SHOW GRANTS TO RECIPIENT <recipient_name>`**: Displays all shares which the recipient can access. To run this command you must be an administrator. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Create a Recipient
-- MAGIC 
-- MAGIC A recipient can be a Databricks user or an external user. For recipients who are not Databricks users, an authentication file is generated which can be shared with recipient and they can download it. **In this demo, we will use Databricks-to-Databricks data sharing, therefore, we need to define user identifier and Databricks takes care of the token management process.**
-- MAGIC 
-- MAGIC 📌 You need a **share identifier** to create a recipient. We will use this account in the next demo to access data. Replace the `<sharing_identifier>` with the recipient identifier.
-- MAGIC 
-- MAGIC Sharing Identifier is the global unique identifier of a Unity Catalog metastore owned by the data recipient with whom you’d like to share data. It has a format of <cloud>:<region>:<uuid>. Example: aws:eu-west-1:b0c978c8-3e68-4cdf-94af-d05c120ed1ef. 
-- MAGIC 
-- MAGIC A Databricks user can find their sharing identifier in the UI or running the a command as;
-- MAGIC   - UI: A user can view their Sharing Identifier in **Shared with me page** (Data → Delta Sharing → Shared with me).
-- MAGIC   - Command: `SELECT CURRENT_METASTORE();`. This command will display the metastore id which is used as sharing identifier. 

-- COMMAND ----------

-- This command will show your sharing identifier.
-- NOTE: This is NOT the identifier that you will use in next code cell. You should obtain the recipient's sharing identifier and use that

SELECT CURRENT_METASTORE();

-- COMMAND ----------

-- Create recipient using Databricks Sharing Identifier
-- 

CREATE RECIPIENT IF NOT EXISTS flights_data_recipient
  USING ID "<sharing_identifier>"
  COMMENT "Flights data recipient (a Databricks User)";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Manage Recipients
-- MAGIC 
-- MAGIC Let's view all recipients and then show the details of the recipient that we have just created. 
-- MAGIC 
-- MAGIC We can display all shares that a recipient has access to using ``

-- COMMAND ----------

-- View all recipients

SHOW RECIPIENTS;

-- COMMAND ----------

-- We can view the details of a specific recipient

DESCRIBE RECIPIENT flights_data_recipient;

-- COMMAND ----------

-- View all Shares for a recipient. Note: You must be an admin to run this command
-- The recipient does not have any permissions yet

SHOW GRANTS TO RECIPIENT flights_data_recipient;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Grant Access and Manage Access Level
-- MAGIC 
-- MAGIC We have created a *share* and a *recipient*. The next step is to make sure the *recipient* has access permissions on the *share*. 
-- MAGIC 
-- MAGIC Following commands can be used to manage permissions;
-- MAGIC 
-- MAGIC - **`GRANT SELECT ON SHARE <share_name> TO RECIPIENT <recipient_name>`**: Grant select permission on a share for the specified recipient.
-- MAGIC 
-- MAGIC - **`REVOKE SELECT ON SHARE <share_name> FROM RECIPIENT <recipient_name>`**: Revoke permission on a share for the specified recipient.

-- COMMAND ----------

-- Grant permissions to the share we just created

GRANT SELECT 
  ON SHARE flights_data_share
  TO RECIPIENT flights_data_recipient;

-- COMMAND ----------

-- Check if recipient can access to the share. You should see Flights_Data_Recipient with SELECT privilege  

SHOW GRANT ON SHARE flights_data_share;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Sharing Partial Data
-- MAGIC 
-- MAGIC In some cases, instead of sharing the whole table, we may need to share partial data. To share only part of a table when adding the table to a share, you can provide a partition specification. The following example shares part of the data in the inventory table, given that the table is partitioned origin column.
-- MAGIC 
-- MAGIC In this example, we will share flights which departured from JFK. You may want to use multiple conditions as well.

-- COMMAND ----------

-- Share flights data that origin is JFK

ALTER SHARE flights_data_share
  ADD TABLE db_flights.flights
  PARTITION (origin = "JFK") AS db_flights.flights_from_jfk;

-- COMMAND ----------

-- Check tables in the share. You should see the new partial data table.

SHOW ALL IN SHARE flights_data_share;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC 📌 Note: We have already granted access for the recipient (flights_data_recipient) to the share (flights_data_share). Therefore, the recipeint will have access to the all tables that we add to the share. In this example, the recipient will have access to partial table.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Share Data with Change Data Feed (CDF)
-- MAGIC 
-- MAGIC Change data feed allows Databricks to track row-level changes between versions of a Delta table. When enabled on a Delta table, the runtime records change events for all the data written into the table. This includes the row data along with metadata indicating whether the specified row was inserted, deleted, or updated.
-- MAGIC 
-- MAGIC CDF is not enabled for tables by default. We must explicitly enable the change data feed option. In this demo, the CDF is already enabled for `flights` table in setup code by setting the table property `delta.enableChangeDataFeed = true` in the `ALTER TABLE` command. 
-- MAGIC 
-- MAGIC In this demo, we will share the `flights` table with CDF option and the recipient will be able to query table data by version, starting from current table version.

-- COMMAND ----------

-- Share flights data table with CDF

ALTER SHARE flights_data_share
  ADD TABLE db_flights.flights
  COMMENT "Flights table with CDF enabled"
  AS db_flights.flights_with_cdf
  WITH CHANGE DATA FEED;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC Let's add a new record to create a new version of the table. The recipient can query the table by version.

-- COMMAND ----------

-- Insert a new record
INSERT INTO db_flights.flights VALUES(1011245, 10, 500, "JFK", "LAX");


-- COMMAND ----------

-- View changes as of version 1

SELECT * FROM table_changes('db_flights.flights', 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Clean up Classroom
-- MAGIC 
-- MAGIC > 📌 **IMPORTANT: Running the code below will delete the catalog and tables used for data sharing. You should run this code block after completing the next lesson, which cover data recipient's data access process.**
-- MAGIC 
-- MAGIC 
-- MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # remove shares and recipients
-- MAGIC spark.sql("DROP SHARE IF EXISTS flights_data_share")
-- MAGIC spark.sql("DROP RECIPIENT IF EXISTS flights_data_recipient")
-- MAGIC 
-- MAGIC # Remove lesson resourses
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC In this demo, we went over the steps to share data using Databricks SQL. The first step is to create a share and add tables to the share. Next, we showed how to create a recipient and grant permissions for the recipient. As we demonstrated Databricks-to-Databricks sharing, we used unique identifier of the recipient to grant access. In the next demo, we will show how to access the shared data.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
