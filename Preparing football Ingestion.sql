-- Databricks notebook source
--create catalog  if not exists football;
use catalog english_football;
create schema if not exists uk_data;
use schema uk_data;
create table  if not exists games using delta;
ALTER TABLE english_football.uk_data.games SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')



-- COMMAND ----------

-- MAGIC %md 
-- MAGIC <h1> Autoloader with Volumes </H1>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC df=spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv").option("cloudFiles.schemaHints", "Round int, Date String, `Team 1` String, `Team 2` string").option("cloudFiles.schemaLocation","dbfs:/hiss").load("/Volumes/english_football/uk_data/data")
-- MAGIC df.writeStream.format("delta").trigger(availableNow=True).option("mergeSchema", "true").option("checkpointLocation", "dbfs:/hiss").toTable("english_football.uk_data.games")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1> Query the Data </h1>

-- COMMAND ----------

select * from english_football.uk_data.games;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <h1> Transform the data</h1>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import split, col
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC
-- MAGIC df=spark.table("english_football.uk_data.games").drop("_rescued_data").select("*",split(col("FT"),"-").alias("score")).drop("FT").withColumnRenamed("Team 1","Team1").withColumnRenamed("Team 2","Team2")
-- MAGIC tf=df.withColumn("Score_T1", df.score[0]).withColumn("Score_T2", df.score[1]).drop("score").select("*",split(col("Date")," ").alias("Date_con")).drop("date")
-- MAGIC tf2=tf.withColumn("DayOfWeek", tf.Date_con[0]).withColumn("Month", tf.Date_con[1]).withColumn("Day", tf.Date_con[2]).withColumn("Year", tf.Date_con[3]).drop("Date_con")
-- MAGIC tf2.write.mode("overwrite").saveAsTable("english_football.uk_data.games_cleaned")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Data Exploration

-- COMMAND ----------

-- DBTITLE 1,Schema of the table
describe english_football.uk_data.games_cleaned

-- COMMAND ----------

-- DBTITLE 1,Number of games played in 10 years 
select count(*) as games_count from english_football.uk_data.games_cleaned

-- COMMAND ----------

-- DBTITLE 1,Draw Game per year
select count(*) as draw_game,year from english_football.uk_data.games_cleaned where Score_T1==Score_T2 group by Year 

-- COMMAND ----------

-- DBTITLE 1,Away win per year
select count(*) as away_win_game,year from english_football.uk_data.games_cleaned where Score_T1<Score_T2 group by Year 

-- COMMAND ----------

-- DBTITLE 1,Home win per year
select count(*) as home_win_game,year from english_football.uk_data.games_cleaned where Score_T1>Score_T2 group by Year 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Sample Queries

-- COMMAND ----------

-- DBTITLE 1,Chelsea Home Win in season 2018/2019
select * from english_football.uk_data.games_cleaned where Team1="Chelsea FC"  and  Score_T1>Score_T2  and Year in (2018,2019)

-- COMMAND ----------

-- DBTITLE 1,Chelsea's game where they won by at least 4 goals of differences
select * from english_football.uk_data.games_cleaned where Team1="Chelsea FC"  and  Score_T1>Score_T2+3  union  select * from english_football.uk_data.games_cleaned  where Team2="Chelsea FC"  and  Score_T1+3<Score_T2 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insights

-- COMMAND ----------

-- DBTITLE 1,Chelsea Wins per  year
select count(*) as wins, year ,"Home Win" as status  from english_football.uk_data.games_cleaned  where Team1="Chelsea FC"  and  Score_T1>Score_T2 group by year   union  select count(*) as wins, year ,"Away Win" as status  from english_football.uk_data.games_cleaned where Team2="Chelsea FC"  and  Score_T1<Score_T2 group by year

-- COMMAND ----------

-- DBTITLE 1,Chelsea Draws per year
select count(*) as draws, year ,"Home draw" as status  from english_football.uk_data.games_cleaned where Team1="Chelsea FC"  and  Score_T1==Score_T2 group by year   union  select count(*) as draws, year ,"Away draw" as status  from english_football.uk_data.games_cleaned where Team2="Chelsea FC"  and  Score_T1==Score_T2 group by year

-- COMMAND ----------

select sum(goals) as goals_per_season, year from ( select sum(Score_T1) as Goals, year ,"Home Goals" as status  from english_football.uk_data.games_cleaned  where Team1="Chelsea FC" group by Team1 ,year  union  select sum(Score_T2) as Goals, year ,"Away Goals" as status  from english_football.uk_data.games_cleaned where Team2="Chelsea FC"  group by Team2, year) group by year

-- COMMAND ----------

select * from english_football.uk_data.games_cleaned;
