-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Sharing Data with External Recipients using Databricks SQL 
-- MAGIC 
-- MAGIC In this course we are going to demonstrate the data sharing process with external users. In this typical scenario, the data *provider* is a Databricks user and the *recipient* can connect to the data using a Delta Sharing *connector*. 
-- MAGIC 
-- MAGIC Databricks-managed Delta Sharing allows administrators to create and manage providers, shares, and recipients with a simple-to-use UI and SQL commands. In the previous course, we demonstrated how to share and access data using both Databricks UI and SQL commands. In this course, we are going to use Databricks SQL, but you can follow along using the UI as well.  
-- MAGIC 
-- MAGIC **Learning Objectives:**
-- MAGIC - Explain the difference between internal and external data sharing
-- MAGIC - Share data externally using Databricks SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Prerequisites
-- MAGIC 
-- MAGIC In this lesson, we are going to share data with external recipients. As Delta Sharing is integrated with Unity Catalog (UC), there are couple prerequisites that need you need to ensure before starting this course.
-- MAGIC 
-- MAGIC - Unity Catalog must be enabled, and at least one metastore must exist.
-- MAGIC - Delta sharing must be enabled for the metastore that you want to use for this course.
-- MAGIC - Only a metastore admin or account admin can share data using Delta Sharing. Therefore, make sure that you have necessary permissions to share data.
-- MAGIC - **External data sharing must be enabled** for your account. The external data sharing enablement process is covered in the "Overview of Delta Sharing" course. 
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

-- MAGIC %run ../Includes/Classroom-Setup-3.1

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
-- MAGIC ## Create A Share
-- MAGIC 
-- MAGIC The first of data sharing process is to create a share. In Delta Sharing, a share is a named object that contains a collection of tables in a metastore that you wish to share as a group. A share can contain tables from only a single metastore. You can add or remove tables from a share at any time.
-- MAGIC 
-- MAGIC *Share* creation and management process is the same as sharing data within Databricks. Share creation and management was covered in depth in the "Sharing Data within Databricks with Delta Sharing" course.

-- COMMAND ----------

-- Create a share to be shared with external recipient

CREATE SHARE IF NOT EXISTS flights_data_share_ext 
COMMENT 'Delta Sharing share for flights data to share with external recipient.';

-- COMMAND ----------

-- Let's check the share and review the details

DESCRIBE SHARE flights_data_share_ext;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Add Tables to the Share 
-- MAGIC 
-- MAGIC As next step fo data sharing, we need to add table(s) to the share. A share can have multiple tables. To keep it simple, we will add only one table with CDF (Change Data Feed) is enabled. This will allow the recipient to query shared table by verison. 

-- COMMAND ----------

-- Add flights table to the share

ALTER SHARE flights_data_share_ext
  ADD TABLE db_flights_ext.flights_ext
  COMMENT "Flights table with CDF enabled to be shared with external recipient"
  WITH CHANGE DATA FEED;

-- COMMAND ----------

-- Check the tables in the share. This should show details of the table we just added to the share. Note the `partitions` and `cdf_shared` options.

SHOW ALL IN SHARE flights_data_share_ext;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a New Version of the Table
-- MAGIC 
-- MAGIC As we enabled CDF for the shared table, let's insert a new record to create a new version of the table. The recipient will see the version details and can query based the table by version. 

-- COMMAND ----------

-- Insert a new record
INSERT INTO db_flights_ext.flights_ext VALUES(1011245, 10, 500, "JFK", "LAX");

-- COMMAND ----------

-- View changes as of version 1

SELECT * FROM table_changes('db_flights_ext.flights_ext', 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Create An External Recipient
-- MAGIC 
-- MAGIC A recipient represents an organization with whom to share data. A recipient can be a Databricks user or an external user. For recipients who are not Databricks users, an authentication file is generated which can be shared with recipient and they can download it.
-- MAGIC 
-- MAGIC The credentials are typically saved as a file containing access token. The Delta Server identify and authorize consumer based on these identifiants.
-- MAGIC 
-- MAGIC **📌 Note that the "activation link" is single use. You can only access it once (it'll return null if already used). Make sure to save it in a secure place.**

-- COMMAND ----------

-- Create an external recipient

CREATE RECIPIENT IF NOT EXISTS flights_data_recipient_ext
  COMMENT "Flights data recipient (External User)";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC The authentication file is shown in **"activation_link"** field of the recipient. The activation link can be viewed in the recipients details page on Databricks as well.

-- COMMAND ----------

--  View Recipient details. Note the "activation link" 

DESC RECIPIENT flights_data_recipient_ext;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Provide Read Access to the Recipient
-- MAGIC 
-- MAGIC We have created a *share* and a *recipient*. The last step is to make sure the *recipient* has access permissions on the *share*. 

-- COMMAND ----------

-- Grant permissions to the share we just created

GRANT SELECT 
  ON SHARE flights_data_share_ext
  TO RECIPIENT flights_data_recipient_ext;

-- COMMAND ----------

-- Check if recipient can access to the share. You should see Flights_Data_Recipient with SELECT privilege  

SHOW GRANT ON SHARE flights_data_share_ext;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Clean up Classroom
-- MAGIC 
-- MAGIC > 📌 **IMPORTANT: Running the code below will delete the catalog and tables used for data sharing. You should run this code block after completing the next lessons, which cover data recipient's data access process.**
-- MAGIC 
-- MAGIC 
-- MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # remove shares and recipients
-- MAGIC spark.sql("DROP SHARE IF EXISTS flights_data_share_ext")
-- MAGIC spark.sql("DROP RECIPIENT IF EXISTS flights_data_recipient_ext")
-- MAGIC 
-- MAGIC # Remove lesson resourses
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC In this demo, we went over the steps to share data externally using Databricks SQL. The first step is to create a share and add tables to the share. Next, we showed how to create a recipient and grant permissions for the recipient. As the recipient is external user, an activation link is created for the recipient, which should be downloaded and shared with the recipient to access the data. In the next demo, we will show how to access the shared data using Delta Sharing connectors.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
