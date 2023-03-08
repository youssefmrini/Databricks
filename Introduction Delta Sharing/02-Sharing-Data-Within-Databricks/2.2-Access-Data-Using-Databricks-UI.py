# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Access Data using Databricks UI
# MAGIC 
# MAGIC In the previous demo, we shared a table using a share and granted read access to our recipient account. In this demo, we will demonstrate how to access the shared data using Databricks UI. Databricks supports UI and Databricks SQL commands to access shared data. We will use SQL commands for the same workflow demonstrated here in next demos.
# MAGIC 
# MAGIC **In this demo, we will show data access process using UI and in next video we will use SQL commands.**
# MAGIC 
# MAGIC **Learning Objectives:**
# MAGIC 
# MAGIC - Describe the data access process for shared data
# MAGIC - Access shared data using the UI
# MAGIC - View and manage shared data using the UI

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC In order to have access to shared data, your account must have necessary permissions and UC catalog enabled. The prerequisites as follow;
# MAGIC 
# MAGIC - Unity Catalog must be enabled, and at least one metastore must exist.
# MAGIC - Delta sharing must be enabled for the metastore that you want to use for this course.
# MAGIC - You need to create a new catalog for the shared data. Therefore, make sure that you have necessary permissions to create a catalog in UC metastore.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Sharing Identifier
# MAGIC 
# MAGIC In order to allow a data provider on Databricks to share data with you through Delta Sharing, they would need to know a globally unique identifier of the Unity Catalog metastore where you are going to access the shared data. It has a format of `<cloud>:<region>:<uuid>`. You can acquire that globally unique identifier by visiting **Shared with me** page (Data → Delta Sharing → Shared with me).
# MAGIC 
# MAGIC In the previous demo, we used this unique metastore identifier to share data with our recipient account. 
# MAGIC 
# MAGIC ## View Providers and Shared Data
# MAGIC 
# MAGIC A provider is a named object that represents the data provider in the real world who shares the data with you. For a data provider in Databricks, the provider object has an authentication type of `DATABRICKS`, which suggests that it uses Databricks-managed Delta Sharing to share the data. For data providers who use the open source protocol and recipient profile authentication method to share the data, its provider object has an authentication of `TOKEN`. 
# MAGIC 
# MAGIC When a data provider shares data with your current Unity Catalog metastore, provider objects are automatically created under the metastore. To view available data providers under your Unity Catalog metastore;
# MAGIC 
# MAGIC - Navigate to **Shared with me** screen (Data → Delta Sharing → Shared with me)
# MAGIC - The **Providers** tab shows a list of providers who shared the data with you.
# MAGIC - Click on the provider name to view **shares***.* The provider name is the *organization name* defined in Databricks account console of account shared data with you. A share is a collection of datasets shared by the data provider. **A share belongs to a data provider and you need to create a catalog from a share to access the dataset inside.**
# MAGIC - Click on **Create Catalog** button for the share you want to access.
# MAGIC     - Enter "flights_data_catalog" as **Catalog Name**
# MAGIC     - Enter Comment as desired
# MAGIC     - Click on **Create** button
# MAGIC - This will create a new catalog and you will see **Schemas** and **Tables** that are shared with you.
# MAGIC - You can view Catalog, Schema(s) and Table(s) details and access sample data in **Data Explorer (**Data → Data Explorer**).**
# MAGIC - If you want to grant permissions for the shared Schema(s) or Table(s) to another user or a group, you can set these permissions in **Permissions** tabs.
# MAGIC 
# MAGIC 📝 **Note that the Catalog Type of this catalog is DELTASHARING and it is read-only. Which means data inside a Delta Sharing Catalog are read-only and can not be created, modified or deleted. You can perform read operations like `DESC`, `SHOW`, `SELECT`but can’t perform write or update operations like `MODIFY`, `UPDATE`, or `DROP`. The only exception to this rule is that the owner of the data object or the metastore admin can update the owner of the data objects to other users or groups.**

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean up Classroom
# MAGIC 
# MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

# COMMAND ----------

# remove catalog

spark.sql("DROP CATALOG IF EXISTS flights_data_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Conclusion
# MAGIC 
# MAGIC In this demo, we went over the steps to access shared using Databricks UI. In order to access data, first, you need to make sure that you have required permissions on metastore. Then, you need to create a catalog for a *share.* Created catalog and accessed data are read-only.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
