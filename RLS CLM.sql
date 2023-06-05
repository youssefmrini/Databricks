-- Databricks notebook source
Create catalog global_employee;
use catalog global_employee;
create schema europe;
use schema europe;

-- COMMAND ----------

CREATE TABLE paris_office (id INT, name STRING, age INT, city String, role String,level String, zip_code String);

-- COMMAND ----------

insert into paris_office values (1,'Youssef Mrini',29,'Paris','SSE','L4','75008');
insert into paris_office values (2,'Clement Lacoudre',33,'Paris','SA','L5','75008');
insert into paris_office values (3,'Quentin Ambard',38,'Paris','LSA','L6','75008');
insert into paris_office values (4,'wissam',38,'Paris','SA','L6','75008');

-- COMMAND ----------

select * from paris_office;

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS permissions(role STRING)
RETURN IF(IS_MEMBER('SA-team'), true, role="SA") or IF(IS_MEMBER('SSE-team'), true, role="SSE");

-- COMMAND ----------

ALTER TABLE europe.paris_office SET ROW FILTER global_employee.europe.SSE ON (role);

-- COMMAND ----------

select * from paris_office

-- COMMAND ----------

CREATE FUNCTION IF NOT EXISTS global_employee.europe.employee_md5(name STRING)
RETURN IF(IS_MEMBER('ANALYST_USA'), name, md5(name));

-- COMMAND ----------

ALTER TABLE europe.paris_office ALTER COLUMN name SET MASK .europe.paris_office.employee_md5
