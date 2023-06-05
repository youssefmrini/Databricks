-- Databricks notebook source

--Apply tags to catalogs
ALTER CATALOG demo_uc SET TAGS ("boat", "example");

-- COMMAND ----------

--Apply tags to Databases
ALTER schema boat SET TAGS ("owner:youssef", "example");

-- COMMAND ----------

--Apply tags to tables
ALTER table titanic_v2 SET TAGS ("dataset");

-- COMMAND ----------

--Set/unset tags for table column
ALTER TABLE titanic_v2 ALTER COLUMN Name SET TAGS ( "PII_titanic");
Alter Table titanic_v2 alter COLUMN Age set TAGS ("PII_titanic","Classified");

-- COMMAND ----------

--Set/unset tags for table column
ALTER TABLE titanic_v2 ALTER COLUMN Age UNSET TAGS ( "PII_titanic" )
