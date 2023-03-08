# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Sharing Data using Databricks UI
# MAGIC 
# MAGIC 
# MAGIC Databricks-managed Delta Sharing allows data providers to share data and data recipients to access the shared data. In this course, we are going to share data within Databricks, which means both data provider and recipient will be Databricks users. 
# MAGIC 
# MAGIC Databricks-managed Delta Sharing allows administrators to create and manage providers, shares, and recipients with a simple-to-use UI and SQL commands.
# MAGIC 
# MAGIC **In this demo, we will show data sharing process using UI and in next video we will use SQL commands.** 
# MAGIC 
# MAGIC **Learning Objectives**
# MAGIC 1. Describe Delta Sharing components and their function for sharing data within Databricks
# MAGIC 1. Create and manage shares using the Databricks UI
# MAGIC 1. Create and manage recipients using the Databricks UI

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC In this lesson, we are going to share data using Delta Sharing. As Delta Sharing is integrated with Unity Catalog (UC), there are couple prerequisites that need you need to ensure before starting this course.
# MAGIC 
# MAGIC - Unity Catalog must be enabled, and at least one metastore must exist.
# MAGIC - Delta sharing must be enabled for the metastore that you want to use for this course.
# MAGIC - Only a metastore admin or account admin can share data using Delta Sharing. Therefore, make sure that you have necessary permissions to share data.
# MAGIC - We need two accounts for this demo. One account will be used for sharing data and the second one will be used for accessing data.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Classroom Setup
# MAGIC 
# MAGIC 
# MAGIC The first thing we're going to do is to run a setup script. This script will define the required configuration variables that are scoped to each user.
# MAGIC 
# MAGIC This script will create a catalog and a schema and import a sample dataset into the schema.  

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-2.1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Dataset Details
# MAGIC 
# MAGIC Run code block below to view necessary details that we will need in this course. Note the catalog name, database name and table for the sample data that we are going to share using Delta Sharing. 
# MAGIC 
# MAGIC In addition, as you progress through this course, you will see various references to the object **`DA`**. This object is provided by Databricks Academy and is part of the curriculum and not part of a Spark or Databricsk API. For example, the **`DA`** object exposes useful variables such as your username and various paths to the datasets in this course as seen here bellow. In this course we are going to use UI mostly, therefore, we are not going to need them for now.

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.database_name}")
print(f"Table Name:        {DA.table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create & Manage Shares
# MAGIC 
# MAGIC The first of data sharing process is to create a share. In Delta Sharing, a share is a named object that contains a collection of tables in a metastore that you wish to share as a group. A share can contain tables from only a single metastore. You can add or remove tables from a share at any time.
# MAGIC 
# MAGIC ### Create A Share
# MAGIC 
# MAGIC - Switch to **Data Science and Engineering workspace**.
# MAGIC - Click on **Data**.
# MAGIC - From Data Explorer left pane, click **Delta Sharing**.
# MAGIC - Select **Shared by me** to see a list current shares or create a new share.
# MAGIC - On the top right, click **Share Data** button.
# MAGIC - Enter *"flights_data_share"* as name of the share.
# MAGIC - Enter any description you want in the comment section.
# MAGIC - Click **Create** button.
# MAGIC 
# MAGIC ### Manage A Share
# MAGIC 
# MAGIC Managing existing shares is done on the same screen as described above. 
# MAGIC 
# MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
# MAGIC - Click on *"flights_data_share"* share you to edit.
# MAGIC - In share details page;
# MAGIC     - You can edit share details such as comment, owner etc.
# MAGIC     - View list of data tables in the share.
# MAGIC     - View list of recipients who have access to the share.
# MAGIC 
# MAGIC ### Add tables to the Share
# MAGIC 
# MAGIC A share is a read-only collection of tables and table partitions to be shared with one or more recipients. Share details page lists data tables and recipients for the selected share. 
# MAGIC 
# MAGIC - To add a table to the share;
# MAGIC     - Click **Edit Tables** button on the top right.
# MAGIC     - Select the catalog (look-up the name from the config variables above) and schema (`db_flights`) to select the `flights` table to add it to the share.
# MAGIC     - In **Advanced table options** you can  configure the *alias, partition, and CDF* for selected tables.
# MAGIC - To grant a recipient access to the share, click **Add Recipient** button on the top right or select the recipient from the dropdown list. Follow steps below to create a new recipient and share data with.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create & Manage Recipients
# MAGIC 
# MAGIC A recipient represents an organization with whom to share data. Grant the recipient access to shares after they are created.
# MAGIC 
# MAGIC ### Create a Recipient
# MAGIC 
# MAGIC To create a *New recipient*;
# MAGIC 
# MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
# MAGIC - Click **New recipient** button.
# MAGIC - Enter *"flights_data_recipient"* as the name of new recipient.
# MAGIC - A recipient can be a Databricks user or an external user. For recipients who are not Databricks users, an authentication file is generated which can be shared with recipient and they can download it. For this example, we will use Databricks-to-Databricks data sharing, therefore, we need to define user identifier and Databricks takes care of the token management process.
# MAGIC - Enter **Sharing Identifier** of the recipient. Sharing Identifier is the global unique identifier of a Unity Catalog metastore owned by the data recipient with whom you’d like to share data. It has a format of `<cloud>:<region>:<uuid>`. Example: `aws:eu-west-1:b0c978c8-3e68-4cdf-94af-d05c120ed1ef`.
# MAGIC - A user can view their **Sharing Identifier** in **Shared with me** page.
# MAGIC - Enter a description you want to define for the recipient in Comment box.
# MAGIC - Click **Create** button.
# MAGIC 
# MAGIC ### Manage Recipients
# MAGIC 
# MAGIC To view and edit recipients;
# MAGIC 
# MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
# MAGIC - Click **Recipients** tab.
# MAGIC - This page lists all recipients along with authentication type, status and creation date. To view a recipient click on recipient name.
# MAGIC - Recipients details page shows list of *Shares* that the recipient has access and *IP Access List*. We will cover these topics in "Delta Sharing Best-Practices" course.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Grant Access and Manage Access Level
# MAGIC 
# MAGIC We have created a *share* and a *recipient*. The next step is to make sure the *recipient* has access permissions on the *share*. 
# MAGIC 
# MAGIC To add a recipient to a share;
# MAGIC 
# MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
# MAGIC - Click on the "flights_data_share" *share*.
# MAGIC - Click on **Add recipient** button on the top right.
# MAGIC - Select the "flights_data_recipient" *recipient* that we created in the previous step.
# MAGIC 
# MAGIC To `revoke` access for a recipient;
# MAGIC 
# MAGIC - Click on the "flights_data_share" *share*.
# MAGIC - Click **Recipients** tab
# MAGIC - Select **Revoke** from the action context menu on the right of the row. This will revoke recipient’s permission for the selected share.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean up Classroom
# MAGIC 
# MAGIC 
# MAGIC > 📌 **IMPORTANT: Running the code below will delete database and tables used for data sharing. You should run this code block after completing the next lesson, which cover data recipient's data access process**
# MAGIC 
# MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

# COMMAND ----------

# remove shares and recipients
spark.sql("DROP SHARE IF EXISTS flights_data_share")
spark.sql("DROP RECIPIENT IF EXISTS flights_data_recipient")

# Remove lesson resourses
DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Conclusion
# MAGIC 
# MAGIC In this demo, we went over the steps to share data using Databricks UI. The first step is to create a share and add tables to the share. Next, we showed how to create a recipient and grant permissions for the recipient. As we demonstrated Databricks-to-Databricks sharing, we used unique identifier of the recipient to grant access. In the next demo, we will show how to access the shared data.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
