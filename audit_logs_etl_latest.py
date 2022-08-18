# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("log_category","<log_category>")
dbutils.widgets.text("azure_subscription_id","<azure_subscription_id>")
dbutils.widgets.text("azure_resource_group","<azure_resource_group>")
dbutils.widgets.text("workspace_name","<workspace_name>")
dbutils.widgets.text("storage_account_access_key","<storage_account_access_key>")
dbutils.widgets.text("storage_account_name","<storage_account_name>")

# COMMAND ----------

## For this Notebook, you will need to create a cluster with the following spark config
##  spark.databricks.unityCatalog.enforce.permissions false
##  spark.databricks.unityCatalog.enabled true
##  spark.databricks.delta.preview.enabled true

# COMMAND ----------

# Configurable variables:

# For production, follow this to set up secret manager.
# https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-get-started.html

# Set up credentials
storage_account_name = dbutils.widgets.get("storage_account_name")
# To use Azure service principal: https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html
storage_account_access_key = dbutils.widgets.get("storage_account_access_key")

log_category = dbutils.widgets.get("log_category")

# Workspace ID (referred to as a resource)
resource_id = "resourceId=/SUBSCRIPTIONS/3f2e4d32-8e8d-46d6-82bc-5bb8d962328b/RESOURCEGROUPS/songkun-demo-rg-DO-NOT-DELETE/PROVIDERS/MICROSOFT.DATABRICKS/WORKSPACES/songkun-demo-uc-deltasharing"



#"resourceId=/SUBSCRIPTIONS/@{dbutils.widgets.get('azure_subscription_id')}/RESOURCEGROUPS/@{dbutils.widgets.get('azure_resource_group')}/PROVIDERS/MICROSOFT.DATABRICKS/WORKSPACES/@{dbutils.widgets.get('workspace_name')}"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We are using hive _temporarily_ as a work aroud for ingestion in the bronze stage. 
# MAGIC -- Once UC supports full python based ETL and external tables with streaming we will alter this workflow.  
# MAGIC USE CATALOG hive_metastore;
# MAGIC CREATE DATABASE IF NOT EXISTS az_audit_logs;
# MAGIC USE az_audit_logs

# COMMAND ----------

# Fixed value, do not change (used for parsing log_category)
container_name = f"insights-logs-{log_category}"

# Path for where the logs are located (constructed dynamically)
log_bucket = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{resource_id}"

# Set up the key for this storage account.
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_access_key)

# Output to the same container.
sink_bucket = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

# Verifies the access is correct.
#dbutils.fs.ls(log_bucket)
dbutils.fs.ls(sink_bucket)
print(log_bucket)


# COMMAND ----------

from pyspark.sql.functions import udf, col, from_unixtime, from_utc_timestamp, from_json, expr
from pyspark.sql.types import StringType, StructField, StructType
import json, time

# COMMAND ----------

streamDF = (
  spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{sink_bucket}/audit_log_schema")
    .option("cloudFiles.includeExistingFiles", True)
    .option("cloudFiles.inferColumnTypes", True)
    .option("cloudFiles.partitionColumns", "")
    .load(log_bucket)
    .withColumn('date',col('time').cast('date'))
)

# COMMAND ----------

bronze_path = f"{sink_bucket}/audit_logs_streaming/bronze"
checkpoint_path = f"{sink_bucket}/checkpoints"

(
  streamDF
    .writeStream
    # Dumping raw JSON records into Delta as a bronze table
    .format("delta")
    .partitionBy("date")
    .outputMode("append")
    # Configure the streaming checkpoint to remember which files have already been processed.
    .option("checkpointLocation", f"{checkpoint_path}/bronze")
    .option("path", bronze_path)
    # Merge the schema of all JSON files since certain JSON records might not contain all the fields.
    .option("mergeSchema", True)
    # Using the "Once" trigger to trigger the streaming job only once and process all the new files so that the job can be executed in an 
    # hourly fashion without needing a long-running cluster for the streaming job.
    # See more details here: https://databricks.com/blog/2017/05/22/running-streaming-jobs-day-10x-cost-savings.html
    .trigger(once=True)
    .start()
)

# COMMAND ----------

# Wait for previous step to finish

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

# Registers the bronze audit log data as an external table
spark.sql(
  f"""
  CREATE TABLE IF NOT EXISTS az_audit_logs.bronze
  USING DELTA
  LOCATION '{bronze_path}'
  """
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Helps by compacting small files 
# MAGIC OPTIMIZE az_audit_logs.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checks whether 
# MAGIC SELECT * FROM az_audit_logs.bronze LIMIT 10

# COMMAND ----------

# Cleaning bronze data into silver data.
bronzeDF = spark.readStream.load(bronze_path)

# Moves nested columns under the 'properties' field to top-level fields
properties_columns = ["actionName", "logId", "requestId", "requestParams", "response", "serviceName", "sourceIPAddress", "userAgent", "sessionId"]
for property in properties_columns:
  bronzeDF = bronzeDF.withColumn(property, col(f"properties.{property}"))

query = (
  # Extracts emai and datetime fields.
  bronzeDF
    .withColumn("email", expr("identity:email"))
    .withColumn("date_time", from_utc_timestamp(from_unixtime(col("time")/1000), "UTC"))
    .drop("identity", "properties", "_rescued_data")
)

# COMMAND ----------

# Writes cleansed datarame to a file.

silver_path = f"{sink_bucket}/audit_logs_streaming/silver"

(
  query
    .writeStream
    .format("delta")
    .partitionBy("date")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/silver")
    .option("path", silver_path)
    .option("mergeSchema", True)
    .trigger(once=True)
    .start()
)

# COMMAND ----------

# Waits for previous step to finish

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

# Registers the silver audit log data as an external table.

spark.sql(
  f"""
  CREATE TABLE IF NOT EXISTS az_audit_logs.silver
  USING DELTA
  LOCATION '{silver_path}'
  """
)

# COMMAND ----------

# Certifies Bronze Table and Silver Table have the same number of rows.

assert(spark.table("az_audit_logs.bronze").count() == spark.table("az_audit_logs.silver").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Compacts small files for better performance
# MAGIC OPTIMIZE az_audit_logs.silver

# COMMAND ----------

# Define helper UDF's for transformation.

def justKeys(string):
  return [i for i in json.loads(string).keys()]

just_keys_udf = udf(justKeys, StringType())

# Transforms and writes the audit log data of a specific service into a gold table.
def flatten_table(service_name, gold_path):
  flattenedStream = spark.readStream.load(silver_path)
  flattened = spark.table("az_audit_logs.silver")
  
  schema = StructType()
  
  keys = (
    flattened
    .filter(col("serviceName") == service_name)
    .select(just_keys_udf(col("requestParams")))
    .alias("keys")
    .distinct()
    .collect()
  )
  
  keysList = [i.asDict()['justKeys(requestParams)'][1:-1].split(", ") for i in keys]
  
  keysDistinct = {j for i in keysList for j in i if j != ""}
  
  if len(keysDistinct) == 0:
    schema.add(StructField('placeholder', StringType()))
  else:
    for i in keysDistinct:
      schema.add(StructField(i, StringType()))
    
  (
    flattenedStream
      .filter(col("serviceName") == service_name)
      .withColumn("requestParams", from_json(col("requestParams"), schema))
      .writeStream
      .partitionBy("date")
      .outputMode("append")
      .format("delta")
      .option("checkpointLocation", f"{checkpoint_path}/gold/{service_name}")
#      .option("path", f"{gold_path}/{service_name}")
      .option("mergeSchema", True)
      .trigger(once=True)
      .table(service_name)
      #.start()
  )
  spark.sql(f"OPTIMIZE az_audit_logs.{service_name}")

# COMMAND ----------

# Collect all the unique audit log categories as a string list.
service_name_list = [i['serviceName'] for i in spark.table("az_audit_logs.silver").select("serviceName").distinct().collect()]
service_name_list

# COMMAND ----------

gold_path = f"{sink_bucket}/audit_logs_streaming/gold"

for service_name in service_name_list:
  flatten_table(service_name, gold_path)

# COMMAND ----------

# Wait for previous step to finish

while spark.streams.active != []:
  print("Waiting for streaming query to finish.")
  time.sleep(5)

# COMMAND ----------

# for service_name in service_name_list:
#   spark.sql(f"""
#   CREATE TABLE IF NOT EXISTS az_audit_logs.{service_name}
#   USING DELTA
#   LOCATION '{gold_path}/{service_name}'
#   """)
#   spark.sql(f"OPTIMIZE az_audit_logs.{service_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN az_audit_logs

# COMMAND ----------

# MAGIC %sql SELECT * FROM az_audit_logs.@{'log_category') ORDER BY FluentdIngestTimestamp DESC LIMIT 10;
