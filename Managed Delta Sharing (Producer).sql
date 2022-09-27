-- Databricks notebook source
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
--CREATE TABLE   disaster.boat.titanics AS SELECT * FROM hive_metastore.default.titanic;

-- COMMAND ----------

alter share finance  
ADD TABLE mutual_fund
COMMENT "Change Data Feed"
WITH CHANGE DATA FEED;

-- COMMAND ----------

ALTER SHARE finance REMOVE TABLE demo_deltasharing.ingestion.mutual_fund;


-- COMMAND ----------

ALTER SHARE finance
ADD TABLE mutual_fund
PARTITION (year = "2020", month = "8");

-- COMMAND ----------

SHOW ALL IN SHARE finance;

-- COMMAND ----------

CREATE RECIPIENT IF NOT EXISTS consumer USING ID 'aws:us-west-2:171cd945-ec1f-47c4-ad88-a37afd9c0f4a';

-- COMMAND ----------

GRANT SELECT ON SHARE finance TO RECIPIENT consumer;

-- COMMAND ----------

REVOKE SELECT
ON SHARE finance
FROM RECIPIENT consumer;
