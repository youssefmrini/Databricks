-- Databricks notebook source

use databricks_youssefmrini;

-- COMMAND ----------

drop database databricks_youssefmrini cascade;
create database databricks_youssefmrini;
use databricks_youssefmrini;

CREATE or replace TABLE  colleagues (name VARCHAR(64) ,age Int, weight Float, address VARCHAR(64), BricksterId INT) 
using delta  

Location '/FileStore/youssefdelta';



-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/FileStore/youssefdelta/_delta_log/")

-- COMMAND ----------

select metaData.*,commitInfo.* from json.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000000.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h1> Insert Data </H1>

-- COMMAND ----------

INSERT INTO colleagues VALUES ('Youssef Mrini',28, 70, '30 rue Paris', 1),('Quentin Ambard',33,65,'30 Rue Lisbone', 2);

SELECT * FROM colleagues;


-- COMMAND ----------

select add.*,commitInfo.* from json.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000001.json`

-- COMMAND ----------

create table   colleagues_paris (name VARCHAR(64) ,age Int, weight Float, address VARCHAR(64), BricksterId INT) using delta  PARTITIONED BY (BricksterId);
insert  into colleagues_paris values ('Hichem Kenniche',35,75,'30 Rue Berlin',4), ('Youssef Mrini',29, 75, '30 rue Paris', 1);


-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.table("colleagues_paris")
-- MAGIC df.createOrReplaceTempView("colleagues_paris")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h1> Merge into </H1>

-- COMMAND ----------

merge into colleagues
using colleagues_paris 
on colleagues.BricksterId=colleagues_paris.BricksterId
when matched then update set *
when not matched then insert *

-- COMMAND ----------

select add.*,commitInfo.* from json.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000002.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h1> Optimize </H1>

-- COMMAND ----------

optimize colleagues

-- COMMAND ----------

select add.*,remove.*,commitInfo.* from json.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000003.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H1> Delete </h1>

-- COMMAND ----------

delete from colleagues where BricksterId=2

-- COMMAND ----------

select commitInfo.*,remove.* from json.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000004.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h1> Overwrite </H1>

-- COMMAND ----------

insert overwrite colleagues values ('Julien Richeux',32,70,'30 rue Londres','6'),('Kedar',34,55,'30 rue Amsterdam','5');


-- COMMAND ----------

select add.*,commitInfo.*, remove.* from json.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000005.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H3> Update the table multiple time to reach 10 commits in order to have the checkpoint </H3>
-- MAGIC
-- MAGIC <br> Insert Into / Delete / Optimize

-- COMMAND ----------

INSERT INTO colleagues VALUES ('Youssef Mrini',28, 70, '30 rue Paris', 1),('Quentin Ambard',33,65,'30 Rue Lisbone', 2);

-- COMMAND ----------

delete from colleagues where age=33 ;

-- COMMAND ----------

optimize colleagues;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <h1> Change the partition </H1>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.table("colleagues")
-- MAGIC df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("BricksterId").saveAsTable("colleagues")

-- COMMAND ----------

select add.*, metaData.* from json.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000009.json`

-- COMMAND ----------


optimize colleagues;

-- COMMAND ----------

use databricks_youssefmrini;
describe history colleagues;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H1> File Added </H1>

-- COMMAND ----------

select add.* from parquet.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000010.checkpoint.parquet`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H1> File Removed </H1>

-- COMMAND ----------

select remove.* from parquet.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000010.checkpoint.parquet`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC <H1> Metadata </H1>

-- COMMAND ----------

select metaData.* from parquet.`dbfs:/FileStore/youssefdelta/_delta_log/00000000000000000010.checkpoint.parquet`

-- COMMAND ----------

delete from colleagues where age=34
