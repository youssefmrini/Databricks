-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Sharing Best Practices: Audit and Monitor Data Sharing
-- MAGIC 
-- MAGIC This lesson does not have a lab/notebook.
-- MAGIC 
-- MAGIC In this lesson, you should be able to describe different types of event types logged for Delta Sharing and identify these events in a audit log file. 
-- MAGIC 
-- MAGIC To enable Delta Sharing logging, **an account admin must set up audit logging for your Databricks account**. 
-- MAGIC 
-- MAGIC Three important fields in a audit log files are;
-- MAGIC 
-- MAGIC - Service name: Events for Delta Sharing are logged with **`serviceName` set to `unityCatalog`**.
-- MAGIC 
-- MAGIC - Action name: `actionName` identifies the type of action
-- MAGIC 
-- MAGIC - Request parameters: The `requestParams` section of each event includes a **`delta_sharing` prefix**.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
