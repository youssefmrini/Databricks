# Databricks notebook source
import mlflow
import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble

spark.sql("create database production_youssef")
spark.sql("use production_youssef")

# Load and preprocess data
white_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-white.csv", sep=';')
red_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-red.csv", sep=';')
white_wine['is_red'] = 0.0
red_wine['is_red'] = 1.0
data_df = pd.concat([white_wine, red_wine], axis=0)
data_df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
data_df = data_df.drop(['quality'], axis=1)


loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/wine_quality_ym_37c6887de4/production",result_type='string')
df=spark.createDataFrame(data_df)
columns = list(data_df.columns)
newsdf=df.withColumn('predictions', loaded_model(*columns))
#finaldf=newsdf.withColumnRenamed("fixed acidity","fixed_acidity").withColumnRenamed("volatile acidity","volatile_acidity").withColumnRenamed("citric acid","citric_acid").withColumnRenamed("residual sugar","residual_sugar").withColumnRenamed("free sulfur dioxide","free_sulfur_dioxide").withColumnRenamed("total sulfur dioxide","total_sulfur_dioxide")
newsdf.write.format("delta").mode("overwrite").saveAsTable("predictions")
display(newsdf)
                                       
                                       

# COMMAND ----------


