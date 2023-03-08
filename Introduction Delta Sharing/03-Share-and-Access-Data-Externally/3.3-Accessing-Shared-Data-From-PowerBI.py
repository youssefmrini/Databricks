# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Accessing Shared Data from PowerBI 
# MAGIC 
# MAGIC In the first demo of this course, we shared *flights* data and granted read access to the *recipient*. In this demo, we are going to demonstrate how external consumers can directly access the shared data using PowerBI.
# MAGIC 
# MAGIC Firstly, we are going to extract the `TOKEN` from the credentials file. Then, we are going to access the shared data using PowerBI. 
# MAGIC 
# MAGIC 
# MAGIC **Learning Objectives:**
# MAGIC - Explain the external data access process
# MAGIC - Access shared data in PowerBI

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC Before moving to the next steps, make sure these prerequisites are met;
# MAGIC 
# MAGIC - Note the recipient *activation_link* or the *credentials file path* generated in the previous lesson. 
# MAGIC - The minimum DBR version required is **DBR 11.2**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Extract `TOKEN` from Credentials File
# MAGIC 
# MAGIC In the previous demo, we downloaded the credentials file and saved it into DBFS. As this file can be downloaded only once, we cannot downloaded it second time. If you have not downloaed the file, you can use the `activation_link` to download the file into your local computer.
# MAGIC 
# MAGIC Follow these steps to extract the `TOKEN`:
# MAGIC 
# MAGIC **If you saved the credentials file to your local computer:**
# MAGIC - Open it with a text editor
# MAGIC - Retrieve the `endpoint URL` and the `bearerToken`
# MAGIC 
# MAGIC **If you saved the credentials file to DBFS in the previous demo:**
# MAGIC - Uncomment the code block below
# MAGIC - Replace the `cred_file_path` with the credentials file path from previous demo
# MAGIC - Retrieve the `endpoint URL` and the `bearerToken`

# COMMAND ----------

import json

# copy-paste the credential file path from previous demo
cred_file_path = "<replace_with_cred_file_path>"

creds = json.loads(dbutils.fs.head(cred_file_path))

print(dbutils.fs.head(cred_file_path))

print(f"Bearer TOKEN: {creds['bearerToken']}")
print(f"Endpoint URL: {creds['endpoint']}")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access Shared from *PowerBI*
# MAGIC 
# MAGIC To connect to Databricks using the Delta Sharing connector, use the following steps:
# MAGIC 
# MAGIC 1. Open Power BI Desktop.
# MAGIC 
# MAGIC 1. Navigate to the **Get Data menu** and **search for Delta Sharing**.
# MAGIC 
# MAGIC 1. Select the connector and then select **Connect**.
# MAGIC 
# MAGIC 1. Enter the **`Endpoint URL`** retrieved from the credentials file in the previous step.
# MAGIC 
# MAGIC 1. Optionally, in the Advanced Options tab you can set a Row Limit for the maximum number of rows you can download. This is set to 1 million rows by default.
# MAGIC 
# MAGIC 1. Select **OK**.
# MAGIC 
# MAGIC 1. In the **Authentication dialog box**, enter **the token retrieved from the credentials file in the `Bearer Token`** field.
# MAGIC 
# MAGIC 1. Select **Connect**.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Conclusion
# MAGIC 
# MAGIC In this demo, we went over the steps to access data externally from PowerBI. PowerBI supports reading shared data from using Delta Sharing connector. First, we retrieved the `TOKEN` and `EnpointURL` from credentials file. Next, we used this information to configure PowerBI.  

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
