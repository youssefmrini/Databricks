# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Enable Delta Sharing
# MAGIC 
# MAGIC In this demo we are going to demonstrate how to enable Delta Sharing on Databricks. First, we are going to enable external data sharing, then, we are going to configure a metastore to enable Delta Sharing.
# MAGIC 
# MAGIC **Learning Objectives**
# MAGIC 
# MAGIC * Enable external data sharing for an account
# MAGIC 
# MAGIC * Configure Delta Sharing on a metastore

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC Before moving forward, please make sure that following requirements are met.
# MAGIC 
# MAGIC - Unity Catalog must be enabled, and at least one metastore must exist.
# MAGIC 
# MAGIC - Account admin role to enable Delta Sharing for a Unity Catalog metastore.
# MAGIC 
# MAGIC - Metastore admin role to share data using Delta Sharing.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Enable Delta Sharing for an account
# MAGIC 
# MAGIC To use Delta Sharing to share data securely with external recipients, an account admin must enable the External Data Sharing feature group for your Databricks account.
# MAGIC 
# MAGIC To enable Delta Sharing;
# MAGIC 
# MAGIC - Login to **account console**.
# MAGIC - In the sidebar, click **Settings**.
# MAGIC - Go to the **Feature enablement** tab.
# MAGIC - On the **External Data Sharing Feature Group** row, click the **Enable** button.
# MAGIC 
# MAGIC You will need to accept applicable terms for Delta Sharing. Clicking Enable represents acceptance of these terms.
# MAGIC 
# MAGIC ## Enable Delta Sharing on a metastore
# MAGIC 
# MAGIC **📌 Note:** You do not need to enable Delta Sharing on your metastore if you intend to use Delta Sharing only to share data with users on other Unity Catalog metastores in your account. Metastore-to-metastore sharing within a single Databricks account is enabled by default.
# MAGIC 
# MAGIC To enable Delta Sharing on a metastore;
# MAGIC 
# MAGIC - Login to **account console**.
# MAGIC - In the sidebar, click **Data**.
# MAGIC - Click the name of a metastore to open its details.
# MAGIC - Click the checkbox next to **Enable Delta Sharing to allow a Databricks user to share data outside their organization**.
# MAGIC - Configure the recipient token lifetime.
# MAGIC 
# MAGIC We are going to dive into token management details in “Delta Sharing Best Practices” section.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Conclusion
# MAGIC 
# MAGIC In this demo, we showed how to enable Delta Sharing for an account and showed how to configure a metastore to enable Delta Sharing. Please note that Unity Catalog must be enabled and at least one metastore must exist. Also, note that these configurations requires admin account privileges.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
