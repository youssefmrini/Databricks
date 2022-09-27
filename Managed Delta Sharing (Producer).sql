-- Databricks notebook source
drop share boat_share;
drop recipient youssef_boat_share;

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS boat_share
COMMENT 'Delta Sharing  for youssef';

-- COMMAND ----------

DESCRIBE SHARE boat_share;

-- COMMAND ----------

use catalog disaster;
use boat;
--CREATE TABLE   disaster.boat.titanics AS SELECT * FROM hive_metastore.default.titanic;

-- COMMAND ----------

describe history disaster.boat.titanics 

-- COMMAND ----------

ALTER SHARE boat_share ADD TABLE disaster.boat.titanics

-- COMMAND ----------

SHOW ALL IN SHARE boat_share;

-- COMMAND ----------

CREATE RECIPIENT IF NOT EXISTS studio USING ID 'aws:us-west-2:171cd945-ec1f-47c4-ad88-a37afd9c0f4a';

-- COMMAND ----------

GRANT SELECT ON SHARE boat_share TO RECIPIENT studio;

-- COMMAND ----------

REVOKE SELECT
ON SHARE boat_share
FROM RECIPIENT studio;
