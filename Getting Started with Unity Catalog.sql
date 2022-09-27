-- Databricks notebook source
-- MAGIC %md 
-- MAGIC 
-- MAGIC <h2> Clean the demo </h2>

-- COMMAND ----------

Drop catalog demo_uc cascade;
--drop catalog demo_uc2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <h2> Create a catalog </H2>

-- COMMAND ----------

create catalog demo_uc;
use catalog demo_uc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Create a Database </h2>

-- COMMAND ----------

create database boat;
use boat;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <h2> Convert a Hive Metastore Table (Managed Table) to Unity Catalog </h2>
-- MAGIC 
-- MAGIC <H4> Managed Tables support Only Delta Tables </H4> 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> CTAS can be used for parquet tables </h2>

-- COMMAND ----------

--SQL

CREATE TABLE demo_uc.boat.titanic_v1
AS SELECT * FROM hive_metastore.default.titanic_parquet;

--%python
--df = spark.table("hive_metastore.default.titanic")
--df.write.saveAsTable("demo_youssef.boat.titanic")


-- COMMAND ----------

CREATE TABLE demo_uc.boat.titanic_v3 using parquet
AS SELECT * FROM hive_metastore.default.titanic_parquet;

-- COMMAND ----------

describe extended demo_uc.boat.titanic_v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Delta Clones can only be used for Delta Tables </h2>

-- COMMAND ----------

CREATE TABLE demo_uc.boat.titanic_v2 deep clone hive_metastore.default.titanic;

-- COMMAND ----------

show grant on demo_uc.boat.titanic_v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Grant Permissions </H2>

-- COMMAND ----------

grant usage, create on catalog demo_uc to `youssef.mrini@databricks.com`;
grant usage, create on schema boat to  `youssef.mrini@databricks.com`;
grant select, modify on table titanic_v2 to  `youssef.mrini@databricks.com`;
grant select, modify on table titanic_v1 to  `youssef.mrini@databricks.com`;

-- COMMAND ----------

show grant on catalog demo_uc

-- COMMAND ----------

show grant on schema boat

-- COMMAND ----------

show grant on titanic_v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <H2> Query the table </H2>

-- COMMAND ----------

select * from titanic_v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <H2> Create an external Delta Table </H2>

-- COMMAND ----------

show external locations

-- COMMAND ----------

describe external location `songkun-uc-external-1`


-- COMMAND ----------

show storage credentials;

-- COMMAND ----------

describe storage credential `songkun-uc-external-1`


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <h4> Grant and manage permissions for  External location </H4>

-- COMMAND ----------

GRANT CREATE TABLE, read files, write files ON external LOCATION `songkun-uc-external-1` TO `youssef.mrini@databricks.com`;
SHOW GRANTS `youssef.mrini@databricks.com` ON external LOCATION `songkun-uc-external-1`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <h4> Grant and manage permissions for Storage Credential </H4>

-- COMMAND ----------

GRANT CREATE TABLE, read files, write files ON STORAGE CREDENTIAL `songkun-uc-external-1` TO `youssef.mrini@databricks.com`;
SHOW GRANTS `youssef.mrini@databricks.com` ON STORAGE CREDENTIAL `songkun-uc-external-1`;


-- COMMAND ----------

create or replace table  demo_uc.boat.titanic_ext 
using delta location "abfss://songkun-uc-external-1@songkunucexternal.dfs.core.windows.net/demo" 
as 
select count(PassengerId) as Nbr, Sex,Pclass, 
       case when Survived=0 then "Dead" 
       else "Survived" 
       end as Status
from demo_uc.boat.titanic_v2 
group by Survived,Sex,Pclass

-- COMMAND ----------

describe extended demo_uc.boat.titanic_ext 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.fs.rm("abfss://songkun-uc-external-1@songkunucexternal.dfs.core.windows.net/parquet",recurse=True)

-- COMMAND ----------

--drop table demo_uc.boat.titanic_ext_parquet ;
create table  demo_uc.boat.titanic_ext_parquet 
using parquet location "abfss://songkun-uc-external-1@songkunucexternal.dfs.core.windows.net/parquet" 
as 
select count(PassengerId) as Nbr, Sex,Pclass, 
       case when Pclass=1 or Pclass=2 then "Rich" 
       else "Poor" 
       end as class
from demo_uc.boat.titanic_v2
group by Survived,Sex,Pclass

-- COMMAND ----------

describe extended demo_uc.boat.titanic_ext_parquet 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h4> Accessing Data </h4>

-- COMMAND ----------

List "abfss://songkun-uc-external-1@songkunucexternal.dfs.core.windows.net/demo" 

-- COMMAND ----------

select * from titanic_ext 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <H2> View </H2>

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC --drop view demo_uc.boat.titanic_redacted;
-- MAGIC --drop view demo_uc.boat.titanic_redacted_row;
-- MAGIC --drop view demo_uc.boat.titanic_redacted_row_v2;

-- COMMAND ----------

-- Column Level Permissions
create view demo_uc.boat.titanic_redacted  as select  Nbr, case when is_account_group_member('EMEA') then Sex else '###' end as Sex, Status from demo_uc.boat.titanic_ext


-- COMMAND ----------


select * from demo_uc.boat.titanic_redacted

-- COMMAND ----------

--Row Level Permissions
create view demo_uc.boat.titanic_redacted_row as select * from demo_uc.boat.titanic_ext where case when is_account_group_member('EMEA') then True else Nbr<5 end


-- COMMAND ----------

select * from demo_uc.boat.titanic_redacted_row

-- COMMAND ----------

create view demo_uc.boat.titanic_redacted_row_v2 as select * from demo_uc.boat.titanic_ext where case when is_account_group_member('CSE-EMEA') then True else Nbr<5 end


-- COMMAND ----------

select * from demo_uc.boat.titanic_redacted_row_v2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h2> Create Delta table with PK and FK </h2>

-- COMMAND ----------

use catalog demo_uc;
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

describe extended persons

-- COMMAND ----------

insert into demo_uc.features.persons values ("Youssef","Mrini","Lord"),("Quentin","ambard","Excellence"),("Laurent","Letturgey","LOSC")

-- COMMAND ----------

insert into demo_uc.features.pets values("booby", "Youssef","Mrini") ,("steeve","Quentin","ambard")
