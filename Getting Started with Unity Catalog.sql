-- Databricks notebook source
-- MAGIC %md 
-- MAGIC 
-- MAGIC <h2> Clean the demo </h2>

-- COMMAND ----------

Drop catalog demo_youssef cascade;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <h2> Create a catalog </H2>

-- COMMAND ----------

create catalog demo_youssef;
use catalog demo_youssef;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Create a Database </h2>

-- COMMAND ----------

create database boat;
use boat;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <h2> Convert a Hive Metastore Table (Managed Table) to Unity Catalog </h2>

-- COMMAND ----------

CREATE TABLE demo_youssef.boat.titanic
AS SELECT * FROM hive_metastore.default.titanic;

-- COMMAND ----------

show grant on demo_youssef.boat.titanic

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Grant Permissions </H2>

-- COMMAND ----------

grant usage, create on catalog demo_youssef to `youssef.mrini@databricks.com`;
grant usage, create on schema boat to  `youssef.mrini@databricks.com`;
grant select, modify on table titanic to  `youssef.mrini@databricks.com`;

-- COMMAND ----------

show grant on demo_youssef.boat.titanic

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <H2> Query the table </H2>

-- COMMAND ----------

select * from demo_youssef.boat.titanic

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <H2> Create an external Delta Table </H2>

-- COMMAND ----------

GRANT CREATE TABLE, read files, write files ON STORAGE CREDENTIAL `songkun-uc-external-1` TO `youssef.mrini@databricks.com`;
SHOW GRANTS `youssef.mrini@databricks.com` ON STORAGE CREDENTIAL `songkun-uc-external-1`;


-- COMMAND ----------

GRANT CREATE TABLE, read files, write files ON external LOCATION `songkun-uc-external-1` TO `youssef.mrini@databricks.com`;
SHOW GRANTS `youssef.mrini@databricks.com` ON external LOCATION `songkun-uc-external-1`;

-- COMMAND ----------

select count(PassengerId) as Nbr, Sex,Pclass, 
case when Survived=0 then "Dead" else "Survived" end as Status from disaster.boat.titanic group by Survived,Sex,Pclass

-- COMMAND ----------

create or replace table  disaster_dec.boat.titanic_ext 
using delta location "abfss://songkun-uc-external-1@songkunucexternal.dfs.core.windows.net/dec" 
as 
select count(PassengerId) as Nbr, Sex,Pclass, 
       case when Survived=0 then "Dead" 
       else "Survived" 
       end as Status
from demo_youssef.boat.titanic 
group by Survived,Sex,Pclass

-- COMMAND ----------

select * from demo_youssef.boat.titanic_ext 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <H2> View </H2>

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Create Delta table with PK and FK </h2>

-- COMMAND ----------

use catalog demo_youssef;
drop database features cascade;

-- COMMAND ----------


create database features;
use features;

-- COMMAND ----------



CREATE TABLE persons(first_name STRING NOT NULL, last_name STRING NOT NULL, nickname STRING,
                       CONSTRAINT persons_pk PRIMARY KEY(first_name, last_name)  DEFERRABLE);


CREATE TABLE pets(name STRING, owner_first_name STRING, owner_last_name STRING,
                    CONSTRAINT pets_persons_fk FOREIGN KEY (owner_first_name, owner_last_name) REFERENCES persons);

-- Create a table with a single column primary key and system generated name
CREATE TABLE customers(customerid STRING NOT NULL PRIMARY KEY, name STRING);

-- Create a table with a names single column primary key and a named single column foreign key
CREATE TABLE orders(orderid BIGINT NOT NULL CONSTRAINT orders_pk PRIMARY KEY,
                      customerid STRING CONSTRAINT orders_customers_fk REFERENCES customers);


-- COMMAND ----------

insert into disaster.features.persons values ("Youssef","Mrini","Lord"),("Quentin","ambard","Excellence"),("Laurent","Letturgey","LOSC")

-- COMMAND ----------

delete from disaster.features.persons where first_name="Youssef"

-- COMMAND ----------

select * from disaster.features.persons
