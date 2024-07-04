-- Databricks notebook source
use catalog delta_talk_dais;
--create schema demo;
use schema demo;

CREATE TABLE personal_infor (
  name STRING,
  age INT
);

-- Insert fake data into the table
INSERT INTO personal_infor VALUES
  ('John', 25),
  ('Jane', 30),
  ('Mike', 35),
  ('Lisa', 28);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC data = [('John', 25, 'New York'), ('Jane', 30, 'San Francisco'), ('Mike', 35, 'Chicago'), ('Lisa', 28, 'Los Angeles')]
-- MAGIC
-- MAGIC # Create the DataFrame
-- MAGIC df = spark.createDataFrame(data, ['name', 'age', 'city'])
-- MAGIC
-- MAGIC # Cast the 'age' column to IntegerType
-- MAGIC df = df.withColumn('age', col('age').cast('integer'))
-- MAGIC
-- MAGIC df.write.mode('overwrite').option('overwriteSchema', 'true').saveAsTable('personal_info')
-- MAGIC
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

--create catalog delta_talk_dais;
use catalog delta_talk_dais;
--create schema demo;
use schema demo;
CREATE TABLE personal_info (
  name STRING,
  age INT,
  city STRING
);

-- Insert fake data into the table
INSERT INTO personal_info VALUES
  ('John', 25, 'New York'),
  ('Jane', 30, 'San Francisco'),
  ('Mike', 35, 'Chicago'),
  ('Lisa', 28, 'Los Angeles');

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("delta_talk_dais.demo.personal_info")
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import functions as F
-- MAGIC df = df.drop("age").drop("status")
-- MAGIC df.write.mode("overwrite").option("overWriteSchema", "true").saveAsTable("delta_talk_dais.demo.personal_info")
-- MAGIC display(df)

-- COMMAND ----------

CREATE TABLE events (
id LONG NOT NULL,
 	date STRING NOT NULL,
 	location STRING,
 	description STRING);
ALTER TABLE events CHANGE COLUMN date DROP NOT NULL;
ALTER TABLE events CHANGE COLUMN id   SET  NOT NULL;
describe detail events

-- COMMAND ----------

CREATE or replace TABLE events_1 (
    id LONG NOT NULL,
    event_date date NOT NULL,
    location STRING,
    description STRING
);



-- COMMAND ----------

ALTER TABLE events_1 ADD CONSTRAINT dateWithinRange CHECK (event_date > '1900-01-01');

-- ALTER TABLE events DROP CONSTRAINT dateWithinRange;

DESCRIBE DETAIL events_1;

-- COMMAND ----------

use catalog delta_talk_dais;
use schema demo;
--drop table customers;
CREATE TABLE customers (
  firstName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  MonthOfBirth int GENERATED ALWAYS AS (month(CAST(birthDate AS DATE))),
  YearOfBirth int GENERATED ALWAYS AS (year(CAST(birthDate AS DATE)))
);

INSERT INTO customers (firstName, lastName, gender, birthDate)
VALUES
  ('John', 'Doe', 'M', '1990-05-15'),
  ('Jane', 'Johnson', 'F', '1988-09-30'),
  ('Michael', 'Williams', 'M', '1985-03-21'),
  ('Emily',  'Brown', 'F', '1992-12-10');

-- COMMAND ----------

select * from customers

-- COMMAND ----------

CREATE TABLE ratings (
  PK BIGINT GENERATED ALWAYS AS IDENTITY (start with 0 increment by 1),
  ratingId INT,
  movieId INT,
  userId INT,
  rating FLOAT,
  timestamp TIMESTAMP
);

-- COMMAND ----------

CREATE TABLE customers_identity (
  PK BIGINT GENERATED ALWAYS AS IDENTITY (start with 0 INCREMENT by 1),
  firstName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  dateOfBirth DATE GENERATED ALWAYS AS (CAST(birthDate AS DATE))
);
INSERT INTO customers_identity (firstName, lastName, gender, birthDate)
VALUES
  ( 'John', 'Doe', 'M', '1990-05-15'),
  ( 'Jane', 'Johnson', 'F', '1988-09-30'),
  ( 'Michael', 'Williams', 'M', '1985-03-21'),
  ('Emily', 'Brown', 'F', '1992-12-10');

-- COMMAND ----------

select * from customers_identity

-- COMMAND ----------

-- Create a table customers_liquid clustered by gender
CREATE TABLE customers_liquid (
  firstName STRING, lastName STRING, gender STRING, birthDate TIMESTAMP, salary INT
) cluster by (gender);

-- Create a table Using CTAS statement
CREATE TABLE customers_liquid cluster by (gender) as select * from customers;

--Optimize/ Cluster the table without specifiying the columns
optimize customers_liquid;

-- COMMAND ----------

describe detail customers_liquid

-- COMMAND ----------

MERGE INTO target
USING source
ON source.key = target.key
WHEN MATCHED THEN
  UPDATE SET target.lastSeen = source.timestamp
WHEN NOT MATCHED THEN
  INSERT (key, lastSeen, status) VALUES (source.key,  source.timestamp, 'active')
WHEN NOT MATCHED BY SOURCE AND target.lastSeen >= (current_date() - INTERVAL '5' DAY) THEN
  UPDATE SET target.status = 'inactive'

-- COMMAND ----------

CREATE TABLE events (
id LONG NOT NULL,
 	date STRING NOT NULL,
 	location STRING,
 	description STRING);

--Adding a Not Null constraint
ALTER TABLE events CHANGE COLUMN date DROP NOT NULL;
--Dropping a Not Null constraint
ALTER TABLE events CHANGE COLUMN id   SET  NOT NULL;


-- COMMAND ----------

CREATE TABLE events (
id LONG NOT NULL,
 	date STRING NOT NULL,
 	location STRING,
 	description STRING);

--Adding a check constraint
ALTER TABLE events ADD CONSTRAINT  dateWithinRange CHECK (date > '1900-01-01');
--Dropping a check constraint
ALTER TABLE events DROP CONSTRAINT dateWithinRange;

