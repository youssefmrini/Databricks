# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop table if exists default.people10m_upload;
# MAGIC CREATE TABLE default.people10m_upload (
# MAGIC   id INT not null,
# MAGIC   firstName STRING comment  "first name",
# MAGIC   middleName STRING,
# MAGIC   lastName STRING,
# MAGIC   gender STRING,
# MAGIC   birthDate TIMESTAMP,
# MAGIC   ssn STRING,
# MAGIC   salary INT
# MAGIC ) USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC alter table default.people10m_upload alter column middleName COMMENT "Middle Name"

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE default.people10m_upload ADD CONSTRAINT salaries CHECK (salary > 0 and salary<1000000);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO default.people10m_upload VALUES
# MAGIC   
# MAGIC   (10, 'Elias', 'Cyril', 'Leadbetter', 'M', '1984-05-22T04:00:00.000+0000', '906-51-2137', 48500),
# MAGIC   (10000000, 'Joshua', 'Chas', 'Broggio', 'M', '1968-07-22T04:00:00.000+0000', '988-61-6247', 90000),
# MAGIC   (9999999, 'Billy', 'Tommie', 'Luppitt', 'M', '1992-09-17T04:00:00.000+0000', '953-38-9452', 0)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe extended default.people10m_upload

# COMMAND ----------

# MAGIC %sql
# MAGIC alter table default.people10m_upload add column id int
