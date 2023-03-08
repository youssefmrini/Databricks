-- Databricks notebook source
create database databricks_paris;

use databricks_paris;

CREATE TABLE colleagues (name VARCHAR(64), address VARCHAR(64), BricksterId INT) using delta  PARTITIONED BY (BricksterId);

INSERT INTO colleagues VALUES ('Youssef Mrini', '30 rue Paris', 111111),('Quentin Ambard','30 Rue Lisbone', 22222);

SELECT * FROM colleagues;


-- COMMAND ----------

drop table colleagues_paris;
create table   colleagues_paris (name VARCHAR(64), address VARCHAR(64), BricksterId INT) using delta  PARTITIONED BY (BricksterId);
insert into   colleagues_paris  table colleagues;
select * from colleagues_paris;

-- COMMAND ----------

insert  into colleagues_paris (name, address, bricksterId) values ('Hichem Kenniche','30 Rue Berlin','44444');
select * from colleagues_paris;

-- COMMAND ----------

insert overwrite colleagues_paris values ('Julien Richeux','30 rue Londres','111111'),('Kedar','30 rue Amsterdam','33333');
select * from colleagues_paris;

-- COMMAND ----------

CREATE TABLE students (name VARCHAR(64), address VARCHAR(64), student_id INT)
  PARTITIONED BY (student_id);

 INSERT INTO students VALUES
    ('Amy Smith', '123 Park Ave, San Jose', 111111);

SELECT * FROM students;


-- COMMAND ----------

INSERT INTO students VALUES
    ('Bob Brown', '456 Taylor St, Cupertino', 222222),
    ('Cathy Johnson', '789 Race Ave, Palo Alto', 333333);

SELECT * FROM students;


-- COMMAND ----------

INSERT INTO students PARTITION (student_id = 444444)
    SELECT name, address FROM persons WHERE name = "Dora Williams";
