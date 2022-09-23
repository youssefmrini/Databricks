-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC # Delta Sharing - Securely share Data with external team / partners
-- MAGIC 
-- MAGIC * Share existing, live data in data lakes / lakehouses (no need to copy it out)
-- MAGIC * Support a wide range of clients by using existing, open data formats (pandas, spark, Tableau etc)
-- MAGIC * Strong security, auditing and governance
-- MAGIC * Efficiently scale to massive datasets
-- MAGIC 
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/delta-sharing/resources/images/delta-sharing-flow.png" width="900px"/>
-- MAGIC 
-- MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fdelta_sharing%2Fgrant&dt=FEATURE_DELTA_SHARING">

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <H2> Create a share </H2>

-- COMMAND ----------

create share if not exists Delta_sharing_lebara comment " This is my first share"

-- COMMAND ----------

show shares

-- COMMAND ----------

describe share Delta_sharing_lebara;

-- COMMAND ----------

show all in share Delta_sharing_lebara;

-- COMMAND ----------

alter share Delta_sharing_lebara add table disaster.boat.titanic

-- COMMAND ----------

alter share Delta_sharing_lebara add table demo_uc.features.pets;
alter share Delta_sharing_lebara add table  demo_uc.features.persons;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <h2> Create a recipient </h2>

-- COMMAND ----------

create recipient  if not exists consumers_lebara comment "my first consumer"

-- COMMAND ----------

describe recipient consumers_lebara

-- COMMAND ----------

show grants on share delta_sharing_lebara 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <h2> Grant access to Consumer </H2>

-- COMMAND ----------

grant select on share delta_sharing_lebara to recipient consumers_lebara

-- COMMAND ----------

-- MAGIC %md <h2> Revoke access to Consumer </H2>

-- COMMAND ----------

REVOKE SELECT
ON SHARE delta_sharing_lebara
FROM RECIPIENT consumers_lebara;

-- COMMAND ----------

-- MAGIC %md <h2> Show Grants on share </H2>

-- COMMAND ----------

show grants on share delta_sharing_lebara


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <H2> Access Activation Link </H2>

-- COMMAND ----------

-- MAGIC %sh pip install delta-sharing

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.put("/sharing/config.share","""{"shareCredentialsVersion":1,"bearerToken":"B25aO35mVCay2BUoFWWU95cofxw_bYzExHuI8X19tsbXjaGSgE3KsQm4vV9eC_kH","endpoint":"https://eastus-c3.azuredatabricks.net/api/2.0/delta-sharing/metastores/0c073ddb-d529-458d-9b22-c1512cb5579e","expirationTime":"9999-12-31T23:59:59.999Z"}
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import delta_sharing
-- MAGIC 
-- MAGIC profile="/dbfs/sharing/config.share"
-- MAGIC client=delta_sharing.SharingClient(profile)
-- MAGIC client

-- COMMAND ----------

-- MAGIC %python
-- MAGIC client.list_all_tables()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC profile="dbfs:/sharing/config.share"
-- MAGIC shareName="delta_sharing_lebara"
-- MAGIC schemaName="boat"
-- MAGIC tableName="titanic"
-- MAGIC 
-- MAGIC tblurl=f"{profile}#{shareName}.{schemaName}.{tableName}"
-- MAGIC df=delta_sharing.load_as_spark(tblurl)
-- MAGIC display(df)
