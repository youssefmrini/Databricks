# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader Schema Inference & Evolution
# MAGIC
# MAGIC
# MAGIC
# MAGIC Starting in DBR 8.2, Databricks Auto Loader has introduced support for schema inference and evolution using Auto Loader. In this notebook, we'll explore a number of potential use cases for this new set of features.
# MAGIC
# MAGIC Note that this functionality is limited to JSON, binary, and text file formats. In this notebook, we'll only be exploring options for working with JSON data.
# MAGIC
# MAGIC Throughout this demo, we'll use "directory listing mode"; if you wish to use ["file notification mode" for file discovery](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html#file-discovery-modes), you'll need to configure additional options.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lesson, you will be able to use Auto Loader to achieve several common ingestion strategies. These include:
# MAGIC * Ingest data to Delta Lake without losing any data
# MAGIC * Rescue unexpected data in well-structured data
# MAGIC
# MAGIC
# MAGIC For detailed reference on Auto Loader and these new features, review:
# MAGIC * [Auto Loader Documentation](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html)
# MAGIC * [Schema Inference & Evolution in Auto Loader Documentation](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-schema.html)

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3> Create a directory  to store the data </h3>

# COMMAND ----------

dbutils.fs.rm("dbfs:/youssefmrini/population", recurse=True)
dbutils.fs.rm("dbfs:/youssefmrini/autoloader", recurse=True)

dbutils.fs.mkdirs("dbfs:/youssefmrini/autoloader/population")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h3> Ingest the data </h3>

# COMMAND ----------

dbutils.fs.cp("/FileStore/population-1.csv","dbfs:/youssefmrini/autoloader",recurse=True)
dbutils.fs.cp("/FileStore/population2.csv","dbfs:/youssefmrini/autoloader",recurse=True)
dbutils.fs.cp("/FileStore/population3.csv","dbfs:/youssefmrini/autoloader",recurse=True)

# COMMAND ----------

dbutils.fs.ls("dbfs:/youssefmrini/autoloader")

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Schema evolution </h3>

# COMMAND ----------

checkpoint_path = 'dbfs:/youssefmrini/population/_checkpoints'
write_path = 'dbfs:/youssefmrini/population'
upload_path='dbfs:/youssefmrini/autoloader'

df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'csv') \
  .option('header', 'true') \
  .option("cloudFiles.schemaLocation",checkpoint_path) \
  .option("cloudFiles.schemaHints","city string, year string, population long ")\
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
  .load(upload_path)


df.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path).option("mergeSchema",True) \
  .start(write_path)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3> Clear the data </h3>

# COMMAND ----------

dbutils.fs.rm("dbfs:/youssefmrini/population", recurse=True)
dbutils.fs.rm("dbfs:/youssefmrini/autoloader", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC <h3> Rescue the data  </h3>

# COMMAND ----------

checkpoint_path = 'dbfs:/youssefmrini/population/_checkpoints'
write_path = 'dbfs:/youssefmrini/population'
upload_path='dbfs:/youssefmrini/autoloader'
# Set up the stream to begin reading incoming files from the
# upload_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'csv') \
  .option('header', 'true') \
  .option("cloudFiles.schemaLocation",checkpoint_path) \
  .option("cloudFiles.schemaEvolutionMode", "rescue") \
  .load(upload_path)


df.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .queryName("information")\
  .start(write_path)

# COMMAND ----------

display(df, streamName='info')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,_rescued_data:money from info

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2> Copy Into </H2>
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create table information ( city string, year string, population string) using delta 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COPY INTO information FROM 'dbfs:/youssefmrini/autoloader' FILEFORMAT = CSV   FORMAT_OPTIONS('header' = 'true') COPY_OPTIONS('mergeSchema'='True')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from information

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Ingest all the files 

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COPY INTO information FROM 'dbfs:/youssefmrini/autoloader' FILEFORMAT = CSV   FORMAT_OPTIONS('header' = 'true') COPY_OPTIONS('mergeSchema'='True','force'='True')
