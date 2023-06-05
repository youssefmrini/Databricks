-- Databricks notebook source
show providers;

-- COMMAND ----------

DESC PROVIDER `tde-azure`;

-- COMMAND ----------

show shares in provider `tde-azure`

-- COMMAND ----------

create catalog external_share using share `tde-azure`.boat

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
