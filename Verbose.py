# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC use  catalog analysis;
# MAGIC create database stats;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop table analysis.stats.notebook

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table analysis.stats.notebook using delta;
# MAGIC COPY INTO analysis.stats.notebook 
# MAGIC FROM 'abfss://insights-logs-notebook@songkunucexternal.dfs.core.windows.net/resourceId=/SUBSCRIPTIONS/3F2E4D32-8E8D-46D6-82BC-5BB8D962328B/RESOURCEGROUPS/SONGKUN-DEMO-RG-DO-NOT-DELETE/PROVIDERS/MICROSOFT.DATABRICKS/WORKSPACES/SONGKUN-DEMO-UC-DELTASHARING/y=*/m=*/d=*/h=*/m=*/'
# MAGIC FILEFORMAT = JSON
# MAGIC COPY_OPTIONS ( 'allowBackslashEscapingAnyCharacter'='true','badRecordsPath'='true', 'mergeSchema'='true');

# COMMAND ----------

# MAGIC %sql
# MAGIC select identity,properties.*,time, actionName from analysis.stats.notebook;

# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *


df=spark.table("analysis.stats.notebook")
schemaIdentity = StructType([StructField("email", StringType(), True),
                    StructField("subjectName", StringType(), True),
                    ],
                    )
schemaParams = StructType([StructField("notebookId", StringType(), True),
                    StructField("executionTime", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("commandId", StringType(), True),  
                    StructField("commandText", StringType(), True),     
                    ],
                    )

new=df.withColumn("identity_new", from_json(col("identity"), schemaIdentity)).select("identity_new.email","properties.*","time")
new.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("analysis.stats.notebook")


# COMMAND ----------

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *


trans=spark.table("analysis.stats.notebook")
schemaParams = StructType([StructField("notebookId", StringType(), True),
                    StructField("executionTime", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("commandId", StringType(), True),  
                    StructField("commandText", StringType(), True),     
                    ],
                    )

finaltable=trans.withColumn("requestParams_new", from_json(col("requestParams"), schemaParams)).select("requestParams_new.*","email","actionName","logId","requestId","requestParams","response","serviceName","sessionId","sourceIPAddress","userAgent","time").drop("requestParams","Host","category","resourceId","operationName","operationVersion","identity","properties")


finaltable.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("analysis.stats.notebook")
#display(finaltable)



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from analysis.stats.notebook

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <h2> Run Command </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from analysis.stats.notebook where actionName="runCommand" 

# COMMAND ----------

# MAGIC %md
# MAGIC <h2> Create Notebook </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC select notebookId, email, logId,time from analysis.stats.notebook where actionName="createNotebook" 

# COMMAND ----------

# MAGIC %md 
# MAGIC <h2> Attach Notebook </H2>

# COMMAND ----------

# MAGIC %sql
# MAGIC select notebookId, email, logId,time from analysis.stats.notebook where actionName="attachNotebook" 

# COMMAND ----------

# MAGIC %md
# MAGIC <h2> log Cluster </H2>

# COMMAND ----------

https://songkunucexternal.blob.core.windows.net/insights-logs-clusters/resourceId=/SUBSCRIPTIONS/3F2E4D32-8E8D-46D6-82BC-5BB8D962328B/RESOURCEGROUPS/SONGKUN-DEMO-RG-DO-NOT-DELETE/PROVIDERS/MICROSOFT.DATABRICKS/WORKSPACES/SONGKUN-DEMO-UC-DELTASHARING/y=2022/m=08/d=11/h=15/m=00/PT1H.json
