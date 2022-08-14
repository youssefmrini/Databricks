-- Databricks notebook source
Create catalog logs;
use  catalog logs;


-- COMMAND ----------

create database stats;
use stats;

-- COMMAND ----------

create table notebook using delta

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC COPY INTO logs.stats.notebook
-- MAGIC FROM 'abfss://insights-logs-notebook@songkunucexternal.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/3F2E4D32-8E8D-46D6-82BC-5BB8D962328B/RESOURCEGROUPS/SONGKUN-DEMO-RG-DO-NOT-DELETE/PROVIDERS/MICROSOFT.DATABRICKS/WORKSPACES/SONGKUN-DEMO-UC-DELTASHARING/y=*/m=*/d=*/h=*/m=*/'
-- MAGIC FILEFORMAT = JSON
-- MAGIC COPY_OPTIONS ( 'allowBackslashEscapingAnyCharacter'='true','badRecordsPath'='true', 'mergeSchema'='true');

-- COMMAND ----------

select * from notebook

-- COMMAND ----------

select properties.* from notebook
--properties.requestParams.notebookId,properties.requestParams.executionTime,
