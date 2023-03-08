// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// DBTITLE 1,Python example
// MAGIC %python
// MAGIC 
// MAGIC from pyspark import Row
// MAGIC   
// MAGIC # Function to upsert `microBatchOutputDF` into Delta table using MERGE
// MAGIC def upsertToDelta(microBatchOutputDF, batchId): 
// MAGIC   # Set the dataframe to view name
// MAGIC   microBatchOutputDF.createOrReplaceTempView("updates")
// MAGIC 
// MAGIC   # ==============================
// MAGIC   # Supported in DBR 5.5 and above
// MAGIC   # ==============================
// MAGIC 
// MAGIC   # Use the view name to apply MERGE
// MAGIC   # NOTE: You have to use the SparkSession that has been used to define the `updates` dataframe
// MAGIC   microBatchOutputDF._jdf.sparkSession().sql("""
// MAGIC     MERGE INTO aggregates t
// MAGIC     USING updates s
// MAGIC     ON s.key = t.key
// MAGIC     WHEN MATCHED THEN UPDATE SET *
// MAGIC     WHEN NOT MATCHED THEN INSERT *
// MAGIC   """)
// MAGIC 
// MAGIC # Setting # partitions to 1 only to make this demo faster.
// MAGIC # Not recommended for actual workloads.
// MAGIC spark.conf.set("spark.sql.shuffle.partitions", "1")
// MAGIC 
// MAGIC # Reset the output aggregates table
// MAGIC spark.createDataFrame([ Row(key=0, count=0) ]).write \
// MAGIC   .format("delta").mode("overwrite").saveAsTable("aggregates")
// MAGIC 
// MAGIC # Define the aggregation
// MAGIC aggregatesDF = spark.readStream \
// MAGIC   .format("rate") \
// MAGIC   .option("rowsPerSecond", "1000") \
// MAGIC   .load() \
// MAGIC   .selectExpr("value % 100 as key") \
// MAGIC   .groupBy("key") \
// MAGIC   .count()
// MAGIC 
// MAGIC # Start the query to continuously upsert into aggregates tables in update mode
// MAGIC aggregatesDF.writeStream \
// MAGIC   .format("delta") \
// MAGIC   .foreachBatch(upsertToDelta) \
// MAGIC   .outputMode("update") \
// MAGIC   .start() \

// COMMAND ----------

// MAGIC %md Check that the data in the Delta table is updating by running the following multiple times.

// COMMAND ----------

display(table("aggregates"))
