-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Access Data using Databricks SQL
-- MAGIC 
-- MAGIC In the previous demo, we shared a table using a share and granted read access to our recipient account. In this demo, we will demonstrate how to access the shared data using Databricks SQL.
-- MAGIC 
-- MAGIC **In this demo, we will show data access process using SQL commands. The same process was demonstrated using UI in "Access Data using Databricks UI" demo.**
-- MAGIC 
-- MAGIC **Learning Objectives:**
-- MAGIC 
-- MAGIC - Describe the data access process for shared data
-- MAGIC - Access shared data using Databricks SQL queries
-- MAGIC - View and manage shared data using Databricks SQL queries
-- MAGIC - Access Change Data Feed using Databricks SQL queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Prerequisites
-- MAGIC 
-- MAGIC In order to have access to shared data, your account must have necessary permissions and UC catalog enabled. The prerequisites as follow;
-- MAGIC 
-- MAGIC - Unity Catalog must be enabled, and at least one metastore must exist.
-- MAGIC - Delta sharing must be enabled for the metastore that you want to use for this course.
-- MAGIC - You need to create a new catalog for the shared data. Therefore, make sure that you have necessary permissions to create a catalog in UC metastore.
-- MAGIC - The minimum DBR version required is **DBR 11.2**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ## Sharing Identifier
-- MAGIC 
-- MAGIC In order to allow a data provider on Databricks to share data with you through Delta Sharing, they would need to know a globally unique identifier of the Unity Catalog metastore where you are going to access the shared data. It has a format of `<cloud>:<region>:<uuid>`. 
-- MAGIC 
-- MAGIC A Databricks user can find their sharing identifier in the UI or running the a command as;
-- MAGIC   - UI: A user can view their Sharing Identifier in **Shared with me page** (Data → Delta Sharing → Shared with me).
-- MAGIC   - Command: `SELECT CURRENT_METASTORE();`. This command will display the metastore id which is used as sharing identifier.
-- MAGIC 
-- MAGIC In the previous demo, we used this unique metastore identifier to share data with our recipient account. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## View and Access Shared Data
-- MAGIC 
-- MAGIC The first step of accessing data is to view the provider and shared data. Then, we need to create a catalog from the share to access the data. 
-- MAGIC 
-- MAGIC A provider is a named object that represents the data provider in the real world who shares the data with you. For a data provider in Databricks, the provider object has an authentication type of `DATABRICKS`, which suggests that it uses Databricks-managed Delta Sharing to share the data. For data providers who use the open source protocol and recipient profile authentication method to share the data, its provider object has an authentication of `TOKEN`. 
-- MAGIC 
-- MAGIC When a data provider shares data with your current Unity Catalog metastore, provider objects are automatically created under the metastore.
-- MAGIC 
-- MAGIC Following commands can be used to view providers and shares, and create a catalog;
-- MAGIC 
-- MAGIC - **`SHOW PROVIDERS`**: Lists all providers that shared data with you.
-- MAGIC 
-- MAGIC - **`DESCRIBE PROVIDER <provider_name>`**: View details of a provider.
-- MAGIC 
-- MAGIC - **`SHOW SHARES IN PROVIDER <provider_name>`**: List shares for a provider.
-- MAGIC 
-- MAGIC - **`CREATE CATALOG IF NOT EXISTS USING SHARE <share_name>`**: Creates a catalog from a share. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### View Providers and Shares
-- MAGIC 
-- MAGIC Let's view all providers and shares. You should see the provider from the previous demo and it should have a share ("Flights_Data_Share") that we created.
-- MAGIC 
-- MAGIC **📌 Note: The provider name is the *organization name* defined in Databricks account console. You will need to replace the <porivder_name> with the name of the provider in the following code blocks.**

-- COMMAND ----------

-- View all providers. Note the name of the provider and use it for next code blocks

SHOW PROVIDERS;

-- COMMAND ----------

-- View provider details

DESCRIBE PROVIDER <replace_with_provider_name>;

-- COMMAND ----------

-- View all availbale shares under a provider object. 
-- You should view the share("Flights_Data_Share") that we created in the previous demo.

SHOW SHARES IN PROVIDER <replace_with_provider_name>;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Create a Catalog and Access Data
-- MAGIC 
-- MAGIC To access the data inside a share, we need to create a catalog from the share. *A share belongs to a data provider and you need to create a catalog from a share to access the dataset inside.*
-- MAGIC 
-- MAGIC **📌 Note that the Catalog Type of this catalog is DELTASHARING and it is read-only. Which means data inside a Delta Sharing Catalog are read-only and can not be created, modified or deleted. You can perform read operations like `DESC`, `SHOW`, `SELECT`but can’t perform write or update operations like `MODIFY`, `UPDATE`, or `DROP`. The only exception to this rule is that the owner of the data object or the metastore admin can update the owner of the data objects to other users or groups.**

-- COMMAND ----------

-- Create a catalog from the share

CREATE CATALOG IF NOT EXISTS flights_data_catalog
  USING SHARE <replace_with_provider_name>.flights_data_share;

-- COMMAND ----------

-- View details of the new catalog

DESCRIBE CATALOG flights_data_catalog;

-- COMMAND ----------

-- View schemas in the catalog

USE CATALOG flights_data_catalog;
SHOW SCHEMAS;

-- COMMAND ----------

-- View tables in the schema

USE SCHEMA db_flights;
SHOW TABLES;

-- COMMAND ----------

-- Select data from the table. You should be able to access the shared table.

SELECT * FROM flights LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Access to Partial Data
-- MAGIC 
-- MAGIC In the previous demo, we shared two tables with the recipient. The first table was the table with the whole data. The second table was partial data with flights departured from JFK. As we have just created a catalog from the share and the share has partial table, we can read data from the partial table. 

-- COMMAND ----------

-- View partial table

SELECT * FROM db_flights.flights_from_jfk;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Access Data with Change Data Feed (CDF)
-- MAGIC 
-- MAGIC In the previous example, we shared the flights table with CDF whihc allows use to query table data by version. Let's view the versions of the table with CDF and query the last version.

-- COMMAND ----------

-- View the table changes

SELECT * FROM table_changes('db_flights.flights_with_cdf', 1)

-- COMMAND ----------

-- Query table by version

SELECT * FROM db_flights.flights_with_cdf VERSION AS OF 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ---

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Clean up Classroom
-- MAGIC 
-- MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

-- COMMAND ----------

-- Remove catalog
DROP CATALOG IF EXISTS flights_data_catalog;
DROP PROVIDER IF EXISTS <replace_with_provider_name>;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC In this demo, we shwoed how to access shared data using Databricks SQL. In order to access data, first, you need to make sure that you have required permissions on metastore. Then, you need to create a catalog for a *share.* Note that created catalog and accessed data are read-only. You can grant access permissions to other users or groups for new catalog, schema and table that we created based on the share.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
