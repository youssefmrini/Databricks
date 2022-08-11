-- Databricks notebook source
show providers;

-- COMMAND ----------

DESC PROVIDER `databricks-uc-demo`;

-- COMMAND ----------

show shares in provider `databricks-uc-demo`

-- COMMAND ----------

create catalog external_share using share `databricks-uc-demo`.boat_share

-- COMMAND ----------


use catalog external_share;
show databases;

-- COMMAND ----------

use boat;

show tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC df=spark.table("external_share.boat.titanic")
-- MAGIC display(df)

-- COMMAND ----------

describe extended external_share.boat.titanic
