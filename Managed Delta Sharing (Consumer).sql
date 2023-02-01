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

val tablePath = "<profile-file-path>#<share-name>.<schema-name>.<table-name>"
val df = spark.readStream.format("deltaSharing")
  .option("startingVersion", "1")
  .option("ignoreChanges", "true")
  .load(tablePath)

-- COMMAND ----------

describe extended external_share.boat.titanic
