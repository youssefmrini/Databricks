-- Databricks notebook source
CREATE SHARE IF NOT EXISTS boat_share 
COMMENT 'Delta Sharing  for youssef';

-- COMMAND ----------

DESCRIBE SHARE boat_share;

-- COMMAND ----------

create catalog disaster;
use catalog disaster;
create schema boat;
use boat;

-- COMMAND ----------

CREATE TABLE disaster.boat.titanic
AS SELECT * FROM hive_metastore.default.titanic;

-- COMMAND ----------

ALTER SHARE boat_share ADD TABLE disaster.boat.titanic;

-- COMMAND ----------

SHOW ALL IN SHARE boat_share;

-- COMMAND ----------

CREATE RECIPIENT IF NOT EXISTS youssef_boat_share USING ID 'azure:eastus:0c073ddb-d529-458d-9b22-c1512cb5579e';

-- COMMAND ----------

GRANT SELECT ON SHARE boat_share TO RECIPIENT youssef_boat_share;

-- COMMAND ----------

ALTER SHARE boat_share ADD TABLE deltasharing.airlinedata.lookupcodes;
