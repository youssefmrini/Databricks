-- Databricks notebook source
drop catalog global_employee cascade;

-- COMMAND ----------

Create catalog global_employee;
use catalog global_employee;
create schema europe;
use schema europe;

-- COMMAND ----------

CREATE TABLE paris_office (id INT, name STRING, age INT, city String, role String,level String, zip_code String);

-- COMMAND ----------

insert into paris_office values (1,'Youssef Mrini',29,'Paris','Marketing','L4','75008');
insert into paris_office values (2,'Clement Lacoudre',33,'Paris','Support','l5','75008');
insert into paris_office values (3,'Quentin Ambard',38,'Paris','Support','L6','75008');
insert into paris_office values (4,'wissam',38,'Paris','Marketing','L6','75008');

-- COMMAND ----------

select * from paris_office;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Add The row filter </H2>

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS permissions(role STRING)
RETURN IF(IS_MEMBER('Marketing'), true, role="Marketing") or IF(IS_MEMBER('Support'), true, role="Support");

-- COMMAND ----------

ALTER TABLE europe.paris_office SET ROW FILTER global_employee.europe.permissions ON (role);

-- COMMAND ----------

select * from paris_office

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Add the mask </H2>

-- COMMAND ----------

CREATE OR REPLACE FUNCTION global_employee.europe.employee_md5(name STRING)
RETURN IF(is_account_group_member('Marketing'), name ,md5(name));

-- COMMAND ----------

ALTER TABLE paris_office ALTER COLUMN name SET MASK global_employee.europe.employee_md5

-- COMMAND ----------

select * from paris_office;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Drop The mask </H2>

-- COMMAND ----------

ALTER TABLE global_employee.europe.paris_office ALTER COLUMN name DROP MASK;

-- COMMAND ----------

select * from paris_office

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Drop The row filter </H2>

-- COMMAND ----------

ALTER TABLE global_employee.europe.paris_office DROP ROW FILTER;

-- COMMAND ----------

select * from paris_office;
