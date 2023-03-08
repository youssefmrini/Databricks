# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Accessing Shared Data using Apache Spark and pandas 
# MAGIC 
# MAGIC In the previous demo, we shared *flights* data and granted read access to the *recipient*. In this demo, we are going to demonstrate how external consumers can directly access the shared data.
# MAGIC 
# MAGIC In this demo, firstly, we are going to save the credentials file into the DBFS (Databricks File System). Then, we are going to access the shared data using Spark, Databricks SQL and Pandas. 
# MAGIC 
# MAGIC 
# MAGIC **Learning Objectives:**
# MAGIC - Explain the external data access process
# MAGIC - Access shared data using PySpark
# MAGIC - Access shared data using Databricks SQL
# MAGIC - Access shared data using pandas

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC Before moving to the next steps, make sure these prerequisites are met;
# MAGIC 
# MAGIC - Note the recipient *activation_link* generated in the previous lesson. 
# MAGIC - The minimum DBR version required is **DBR 11.2**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Delta Sharing Library
# MAGIC 
# MAGIC `delta-sharing` is available as a python package that can be installed via pip. This simplifies the consumer side integration; anyone who can run python can consume shared data via SharingClient object.
# MAGIC 
# MAGIC Run the code cell below to install the Delta Sharing library for python.
# MAGIC 
# MAGIC Note: This  `delta-sharing` library must be installed before classroom setup script.

# COMMAND ----------

# MAGIC %pip install delta-sharing

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Classroom Setup
# MAGIC 
# MAGIC 
# MAGIC The first thing we're going to do is to run a setup script. This script will define the required configuration variables that are scoped to each user.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-3.2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The only config variable that we need in this demo is the *working_dir* path. We are going to use this path to download the credintial file and save it into.

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Working Directory: {DA.paths.working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Download Delta Sharing Credentials
# MAGIC 
# MAGIC When a new Recipient entity is created for a Delta Share an activation link for that recipient will be generated. That URL will lead to a website for data recipients to download a credential file that contains a long-term access token for that recipient. Following the link will be take the recipient to an activation page that looks similar to this:
# MAGIC 
# MAGIC 
# MAGIC From this site the .share credential file can be downloaded by the recipient. This file contains the information and authorization token needed to access the Share. The contents of the file will look similar to the following example.
# MAGIC 
# MAGIC `{"shareCredentialsVersion":1,
# MAGIC "bearerToken":"B-9GD...aeiX",
# MAGIC "endpoint":"https://oregon.cloud.databricks.com/api/2.0/delta-sharing/metastores/XXX",
# MAGIC "expirationTime":"2022-11-27T09:38:03.897Z"}`
# MAGIC 
# MAGIC Due to the sensitive nature of the token, be sure to save it in a secure location and be careful when visualising or displaying the contents. 
# MAGIC 
# MAGIC Note: If you encounter any error while saving the credential file to the DBFS, you need to redone the previous demo as once the file is downloaded it is no possible to download second time.

# COMMAND ----------

# Download the cred file and save it to working directory
import urllib.request

# Use the activation_link from previous demo 
activation_link = "<replace_with_activation_link>"

activation_link = activation_link.replace('delta_sharing/retrieve_config.html?','api/2.0/unity-catalog/public/data_sharing_activation/')
urllib.request.urlretrieve(activation_link, f"/tmp/credential_file.share")
dbutils.fs.mv(f"file:/tmp/credential_file.share", DA.paths.working_dir)
cred_file_path = f"{DA.paths.working_dir}/credential_file.share"

print(f"Your file was downloaded to: {DA.paths.working_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Access Shared Data with *Apache Spark*
# MAGIC 
# MAGIC Delta Sharing library comes with a Apache Spark connector. All we need to do is to use the *credential file* path that we downloaded and load the data as Spark dataframe.
# MAGIC 
# MAGIC To access the shared data we need a properly constructed url. 
# MAGIC 
# MAGIC **📌 The expected format of the url is: `< profile_file \>#< share_name \>.< schema_name \>.< table_name \>`**
# MAGIC 
# MAGIC We are going to use the `share_name`, `schema_name`, and `table_name` from the previous demo.
# MAGIC 
# MAGIC To load the data, we can use `delta-sharing` cleint.

# COMMAND ----------

import delta_sharing
from pyspark.sql.functions import sum, col, count

data_url = f"{cred_file_path}#flights_data_share_ext.db_flights_ext.flights_ext"
spark_flights_df = delta_sharing.load_as_spark(data_url)

display(spark_flights_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access Shared Data with *Databricks SQL* 
# MAGIC 
# MAGIC As a Databricks user, you can experience Delta Sharing using plain SQL directly in your notebook, making data access even easier.
# MAGIC 
# MAGIC It's then super simple to do any kind of queries using the remote table, including joining a Delta Sharing table with a local one or any other operation.
# MAGIC 
# MAGIC The first is to can create a SQL table and **use `'deltaSharing'` as a datasource**.
# MAGIC 
# MAGIC As usual, we need to provide the url as: **`< profile_file >#< share_ >.< database >.< table >`**

# COMMAND ----------

print(f"Credential URL: {cred_file_path}#flights_data_share_ext.db_flights_ext.flights_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Use the file path printed in the previous code block
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS accessed_flights_data
# MAGIC     USING deltaSharing
# MAGIC     LOCATION "<replace_with_cred_url>";

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Select data to see if the data is recieved successfully.
# MAGIC 
# MAGIC SELECT * FROM accessed_flights_data;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Access Shared Data with pandas
# MAGIC 
# MAGIC Accessing data with pandas is similar to access with Apache Spark 
# MAGIC Delta Sharing library comes with a Apache Spark connector. All we need to do is to use the *credential file* path that we downloaded and load the data as Spark dataframe.
# MAGIC 
# MAGIC To access the shared data we need a properly constructed url. 
# MAGIC 
# MAGIC **📌 The expected format of the url is: `< profile_file \>#< share_name \>.< schema_name \>.< table_name \>`**
# MAGIC 
# MAGIC 
# MAGIC **📌 IMPORTANT: The path for credentials file should start with "/dbfs/" instead of "dbfs:/"**
# MAGIC 
# MAGIC We are going to use the `share_name`, `schema_name`, and `table_name` from the previous demo.
# MAGIC 
# MAGIC To load the data, we can use `delta-sharing` cleint.

# COMMAND ----------

import delta_sharing

cred_file_path_pandas = cred_file_path.replace("dbfs:/", "/dbfs/")
data_url = f"{cred_file_path_pandas}#flights_data_share_ext.db_flights_ext.flights_ext"
flights_df = delta_sharing.load_as_pandas(data_url)

flights_df.head(100)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Clean up Classroom
# MAGIC 
# MAGIC > 📌 **IMPORTANT: Running the code below will credential file saved to DBFS file but we need this file in the next demo (Accessing Shared Data from PowerBI). You should run this code block after completing the whole course.**
# MAGIC 
# MAGIC Run the following cell to remove lessons-specific assets created during this lesson.

# COMMAND ----------

# Remove lesson resourses
DA.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Conclusion
# MAGIC 
# MAGIC In this demo, we went over the steps to access data externally using three common Delta Sharing connectors. The first step of accessing data externally is to download the `credential file` and install `delta-sharing` library. The second step is to load the data with delta-sharing client. 

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
