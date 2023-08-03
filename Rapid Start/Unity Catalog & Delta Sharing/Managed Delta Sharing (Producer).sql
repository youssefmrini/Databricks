-- Databricks notebook source
select current_metastore()

-- COMMAND ----------

drop share finance;
drop recipient consumer;

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS finance
COMMENT 'Delta Sharing  for youssef';

-- COMMAND ----------

DESCRIBE SHARE finance;

-- COMMAND ----------

use catalog demo_deltasharing;
use ingestion;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h2> Enable Change Data Feed </h2>

-- COMMAND ----------

alter share finance  
ADD TABLE mutual_fund 
COMMENT "Change Data Feed"
WITH CHANGE DATA FEED;

-- COMMAND ----------

ALTER SHARE <share_name> ADD TABLE <catalog_name>.<schema_name>.<table_name>  [COMMENT "<comment>"]
   [PARTITION(<clause>)] [AS <alias>]
   [WITH HISTORY | WITHOUT HISTORY];

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h2> Removing a table from a share </H2>

-- COMMAND ----------

ALTER SHARE finance REMOVE TABLE demo_deltasharing.ingestion.mutual_fund;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h2> Enable Sharing specific partitions </H2>

-- COMMAND ----------

ALTER SHARE finance
ADD TABLE mutual_fund
PARTITION (year = "2020", month = "8"),
 (year = "2020", month = "9");

-- COMMAND ----------

SHOW ALL IN SHARE finance;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Create a recipient </H2>

-- COMMAND ----------

CREATE RECIPIENT IF NOT EXISTS consumer USING ID 'aws:us-west-2:171cd945-ec1f-47c4-ad88-a37afd9c0f4a';

-- COMMAND ----------

show providers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Grant permissions to the recipient </H2>

-- COMMAND ----------

GRANT SELECT ON SHARE finance TO RECIPIENT consumer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H2> Revoke permissions to the recipient </H2>

-- COMMAND ----------

REVOKE SELECT
ON SHARE finance
FROM RECIPIENT consumer;
