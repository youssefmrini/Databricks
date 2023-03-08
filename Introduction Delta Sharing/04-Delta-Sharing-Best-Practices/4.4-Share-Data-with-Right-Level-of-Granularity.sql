-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Sharing Best Practices: Share Data with Right Level of Granularity
-- MAGIC 
-- MAGIC In the previous lessons we discussed the advantages of sharing data by following the principle of least privilege. Another important best practice for sharing data is to share minimal data such that if a recipient credential is compromised, it is associated with the fewest number of data sets or the smallest subset of the data possible. In addition, in cases when the frequency of writing or reading data is high, we need an efficient sharing method. In this section of the course we are going to discuss how to share data with the right level of granularity to ensure data security and efficiency. 
-- MAGIC 
-- MAGIC 
-- MAGIC Learning Objectives:
-- MAGIC 
-- MAGIC - Explain use cases and benefits of sharing partial data
-- MAGIC 
-- MAGIC - Explain use cases and benefits of sharing with data Change Data Feed (CDF)
-- MAGIC 
-- MAGIC - Share and access a subset of data using partitions
-- MAGIC 
-- MAGIC - Share and access data changes with Change Data Feed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Prerequisites
-- MAGIC 
-- MAGIC In this lesson, we are going to share data with external recipients. As Delta Sharing is integrated with Unity Catalog (UC), there are couple prerequisites that need before starting this course.
-- MAGIC 
-- MAGIC - Unity Catalog must be enabled, and at least one metastore must exist.
-- MAGIC - Delta sharing must be enabled for the metastore that you want to use for this course.
-- MAGIC - Only a metastore admin or account admin can share data using Delta Sharing. Therefore, make sure that you have necessary permissions to share data.
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

-- MAGIC %run ../Includes/Classroom-Setup-4.4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Dataset Details
-- MAGIC 
-- MAGIC Run code block below to view necessary details that we will need in this course. Note the catalog name, database name and table for the sample data that we are going to share using Delta Sharing.
-- MAGIC 
-- MAGIC A sample dataset of flights data is created for you. **The table is partioned by `origin` field and Change Data Feed(CDF) is specified which will allow us to share partial data and share data with CDF enabled.**
-- MAGIC 
-- MAGIC In addition, as you progress through this course, you will see various references to the object **`DA`**. This object is provided by Databricks Academy and is part of the curriculum and not part of a Spark or Databricks API. For example, the **`DA`** object exposes useful variables such as your username and various paths to the datasets in this course as seen here below. In this course we are going to use UI mostly, therefore, we are not going to need them for now.

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
-- MAGIC **📌 Note-1:** Databricks supports sharing data with partitions and CDF using **Data Explorer UI, SQL commands and Unity Catalog CLI**. In this demo, we are going use the UI for defining partition specifications and enabling CDF for a shared dataset.  If you want to follow along using SQL instead of the UI, please refer to **"Sharing Data within Databricks"** course which coveres these using SQL.
-- MAGIC 
-- MAGIC **📌 Note-2:** In this demo, we are going to cover data sharing process without diving into the details of accessing shared data as this is covered in **Access Shared Data Externally with Delta Sharing**. If you want to access shared data with partition specification or CDF enabled, please follow instructions covered in **Access Shared Data Externally with Delta Sharing** course.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Partial Data Sharing
-- MAGIC 
-- MAGIC A share is a read-only collection of tables and table partitions to be shared with one or more recipients. Let's first create a share and then specifiy the partition details while adding a table to the share.
-- MAGIC 
-- MAGIC **📌 Important:** Before sharing a table with partition specification, make sure the table is partitioned by the column. In this demo, the `flights` table created in the setup script is already **partitioned by `origin` column** when it is created. This means we can share partial data of this table by specifiying `origin` column. *However, if we try to share a subset of dataset by `destination` column we will get an error as the table is not paritioned by this column.*   

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Create A Share
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - On the top right, click **Share Data** button.
-- MAGIC 
-- MAGIC - Enter **`flights_partial_data_share`** as name of the share.
-- MAGIC 
-- MAGIC - Enter any description you want in the comment section.
-- MAGIC 
-- MAGIC - Click **Create** button.
-- MAGIC 
-- MAGIC 
-- MAGIC ### Add Partial Data to the Share
-- MAGIC 
-- MAGIC  Share details page lists data tables and recipients for the selected share. To add a table to the share;
-- MAGIC 
-- MAGIC - Click **Add Tables** button on the top right.
-- MAGIC 
-- MAGIC - Select the catalog (look-up the name from the config variables above) and schema (`db_flights`) to select the `flights` table to add it to the share.
-- MAGIC 
-- MAGIC - Click **Advanced table options** to specify a **partition**. 
-- MAGIC 
-- MAGIC - Hover over partition cell and click **Add partition** button. 
-- MAGIC 
-- MAGIC - Enter **`(origin = 'JFK')`** into the textbox. You can define multiple criteria for PARTITION clause. [Refer to PARTITION documentation page for syntax requirements](https://docs.databricks.com/sql/language-manual/sql-ref-partition.html).
-- MAGIC 
-- MAGIC - Click **Save** button.
-- MAGIC 
-- MAGIC Go back to share "Data" tab and make sure the **Partition** column has partition specification as we just defined.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Sharing Data with Change Data Feed (CDF)
-- MAGIC 
-- MAGIC Change Data Feed (CDF) allows Databricks to track row-level changes between versions of a Delta table. When enabled, all insert, delete and update events are recorded and a new version of the table is created.
-- MAGIC 
-- MAGIC **📌 Important: Change data feed is *not* enabled by default.** To share a table with CDF, it should be enabled on the table. In this demo, the initial setup script enables this feature for us. If you want to enable CDF for a table, you can use `ALTER TABLE` SQL command as **`ALTER TABLE <table_name> SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`**. 
-- MAGIC 
-- MAGIC If you are not sure if CDF is enabled or not for a table **you can check this property in table "Details" page in Data Explorer.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ### Add CDF Enabled Data to the Share
-- MAGIC 
-- MAGIC  Share details page lists data tables and recipients for the selected share. To add a table to the share with CDF enabled;
-- MAGIC 
-- MAGIC - Navigate to **Shared by me** screen (Data → Delta Sharing → Shared by me).
-- MAGIC 
-- MAGIC - Click Share Name (**`flights_partial_data_share`**).
-- MAGIC 
-- MAGIC - Click **Edit Tables** button on the top right.
-- MAGIC 
-- MAGIC - Select the catalog (look-up the name from the config variables above) and schema (`db_flights`) to select the `flights` table to add it to the share.
-- MAGIC 
-- MAGIC - Click **Advanced table options**. 
-- MAGIC 
-- MAGIC - Check **Share CDF** checkbox. 
-- MAGIC 
-- MAGIC - Click **Save** button.
-- MAGIC 
-- MAGIC Go back to share "Data" page and make sure the **CDF Shared** column has a check icon.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Clean up Classroom
-- MAGIC 
-- MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # remove shares
-- MAGIC spark.sql("DROP SHARE IF EXISTS flights_partial_data_share")
-- MAGIC 
-- MAGIC # Remove lesson resourses
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Conclusion
-- MAGIC 
-- MAGIC In this section of the course, we demonstrated how to share partial data with partitions and Change Data Feed. We mainly used UI for sharing data but you can use SQL and Unity Catalog CLI too. It is important to note the requirements before sharing partial data and CDF enabled data. For partial data, a partition must be defined on table while creating the table. Similarly, to be able to share a table with CDF enabled, change data feed property must be enabled for the table. Finally, as the main goal of this section was to demonstrate data sharing steps, we didn't cover how to access partial data and CDF enabled data, however, these topics are covered in "Access Shared Data Externally with Delta Sharing" course that you can find detailed instructions and code examples.
-- MAGIC 
-- MAGIC This was the last course for our "Introduction to Delta Sharing" course series. Hope you enjoyed and see you in another course!

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
