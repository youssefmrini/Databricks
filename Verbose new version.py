# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC --create catalog verbose;
# MAGIC use catalog verbose;
# MAGIC --create database stats;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --create table verbose.stats.notebook using delta;
# MAGIC COPY INTO verbose.stats.notebook 
# MAGIC FROM 'abfss://insights-logs-notebook@songkunucexternal.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/3F2E4D32-8E8D-46D6-82BC-5BB8D962328B/RESOURCEGROUPS/SONGKUN-DEMO-RG-DO-NOT-DELETE/PROVIDERS/MICROSOFT.DATABRICKS/WORKSPACES/SONGKUN-DEMO-UC-DELTASHARING/y=*/m=*/d=*/h=*/m=*/'
# MAGIC FILEFORMAT = JSON
# MAGIC COPY_OPTIONS ( 'allowBackslashEscapingAnyCharacter'='true','badRecordsPath'='true', 'mergeSchema'='true');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from verbose.stats.notebook;

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *


df=spark.table("verbose.stats.notebook")
schemaIdentity = StructType([StructField("email", StringType(), True),
                    StructField("subjectName", StringType(), True),
                    ],
                    )

schemaParams = StructType([StructField("notebookId", StringType(), True),
                    StructField("executionTime", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("commandId", StringType(), True),  
                    StructField("commandText", StringType(), True),  
                    StructField("clusterId", StringType(), True)   
                    ],
                    )

new=df.withColumn("identity_new", from_json(col("identity"), schemaIdentity)).select("identity_new.email","properties.*","time")

finaltable=new.withColumn("requestParams_new", from_json(col("requestParams"), schemaParams)).select("requestParams_new.*","email","actionName","logId","requestId","requestParams","response","serviceName","sessionId","sourceIPAddress","userAgent","time").drop("requestParams","Host","category","resourceId","operationName","operationVersion","identity","properties")



conv1=finaltable.select(regexp_replace(col("time"),'T',' ').alias("date1"),"*")
conv2=conv1.select(regexp_replace(col("date1"),'Z','').alias("datefinal"),"*")

fin=conv2.withColumn("date",to_timestamp("datefinal","yyyy-MM-dd HH:mm:ss")).withColumn("month",month("datefinal")).withColumn("year",year("datefinal")).withColumn("minute",minute("datefinal")).withColumn("hour",hour("datefinal")).withColumn("day",dayofmonth("datefinal")).drop("date1","time")
fin.write.mode("append").option("mergeSchema", "true").saveAsTable("verbose.stats.notebooklogs")





# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from verbose.stats.notebooklogs

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2> Run Command </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from verbose.stats.notebooklogs where actionName="runCommand" 

# COMMAND ----------

# MAGIC %md
# MAGIC <h2> Create Notebook </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC select datefinal, notebookId, email,logId,month, year,minute, hour, day from verbose.stats.notebooklogs where actionName="createNotebook" 

# COMMAND ----------

# MAGIC %md 
# MAGIC <h2> Attach Notebook </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC select notebookId, email, logId,datefinal,clusterId,month, year,minute, hour, day from verbose.stats.notebooklogs where actionName="attachNotebook" 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2> Detach Notebook </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC select notebookId, email, logId,datefinal,clusterId,month, year,minute, hour, day from verbose.stats.notebooklogs where actionName="detachNotebook" 

# COMMAND ----------

# MAGIC %md
# MAGIC <h2>  Cluster Logs </H2>

# COMMAND ----------

spark.conf.get("spark.databricks.delta.formatCheck.enabled","false")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC use catalog verbose;
# MAGIC use stats;

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC create table verbose.stats.clusters using delta;
# MAGIC COPY INTO verbose.stats.clusters 
# MAGIC FROM 'abfss://insights-logs-clusters@songkunucexternal.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/3F2E4D32-8E8D-46D6-82BC-5BB8D962328B/RESOURCEGROUPS/SONGKUN-DEMO-RG-DO-NOT-DELETE/PROVIDERS/MICROSOFT.DATABRICKS/WORKSPACES/SONGKUN-DEMO-UC-DELTASHARING/y=*/m=*/d=*/h=*/m=*/'
# MAGIC FILEFORMAT = JSON
# MAGIC COPY_OPTIONS ( 'allowBackslashEscapingAnyCharacter'='true','badRecordsPath'='true', 'mergeSchema'='true');

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *,properties.* from verbose.stats.clusters

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *


transfo=spark.table("verbose.stats.clusters")
schemaIdentity = StructType([StructField("email", StringType(), True),
                    StructField("subjectName", StringType(), True),    
                    ],
                    )

transfor=transfo.withColumn("identity_new", from_json(col("identity"), schemaIdentity)).select("*","identity_new.email", "properties.*")
schemaIdentity = StructType([StructField("clusterId", StringType(), True),
                    StructField("clusterName", StringType(), True),
                    StructField("clusterOwnerUserId", StringType(), True),  
                    StructField("clusterState", StringType(), True),
                    StructField("node_type_id", StringType(), True),
                    StructField("spark_version", StringType(), True),
                    StructField("num_workers", StringType(), True),
                    StructField("data_security_mode", StringType(), True),
                    StructField("idempotency_token", StringType(), True),
                    StructField("custom_tags", StringType(), True),
                    #StructField("num_workers", StringType(), True),
                    StructField("billing_info", StringType(), True),
                    StructField("cluster_event_notification_info", StringType(), True),
                    StructField("spark_conf", StringType(), True),
                    StructField("cluster_creator", StringType(), True),
                    StructField("cluster_source", StringType(), True),
                    StructField("azure_attributes", StringType(), True),
                    StructField("autotermination_minutes", StringType(), True),
                    StructField("enable_elastic_disk", StringType(), True),
                    StructField("disk_spec", StringType(), True)
                                     
                    ],
                    )





finaltable=transfor.withColumn("requestParams_n", from_json(col("requestParams"), schemaIdentity)).select("*","requestParams_n.*").drop("requestParams","identity","properties","requestParams_n","identity_new")
#.select("requestParams_new.*","email","actionName","logId","requestId","requestParams","response","serviceName","sessionId","sourceIPAddress","userAgent","time").drop("requestParams","Host","category","resourceId","operationName","operationVersion","identity","properties")
conv1=finaltable.select(regexp_replace(col("time"),'T',' ').alias("date1"),"*")
conv2=conv1.select(regexp_replace(col("date1"),'Z','').alias("datefinal"),"*")

fin=conv2.withColumn("date",to_timestamp("datefinal","yyyy-MM-dd HH:mm:ss")).withColumn("month",month("datefinal")).withColumn("year",year("datefinal")).withColumn("minute",minute("datefinal")).withColumn("hour",hour("datefinal")).withColumn("day",dayofmonth("datefinal")).drop("date1","time").drop("date","identity_new")
fin.write.mode("append").option("mergeSchema", "true").saveAsTable("verbose.stats.clusterlogs")


display(fin)


