# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

from transformers import pipeline
import pandas as pd
import torch

# COMMAND ----------

import os 
os.environ['TRANSFORMERS_NO_ADVISORY_WARNINGS'] = '1'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Zeroshot inference

# COMMAND ----------

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

zero_shot_pipeline = pipeline(
    task="zero-shot-classification",
    model="MoritzLaurer/deberta-v3-large-zeroshot-v1.1-all-33",
    ##device=device
)

# COMMAND ----------

inference_config = {'candidate_labels': ['politics',
  'finance',
  'sports',
  'science and technology',
  'pop culture',
  'breaking news'],
 'multi_label': False}

# COMMAND ----------

zero_shot_pipeline(text, **inference_config, device=device)

# COMMAND ----------

device

# COMMAND ----------

text = "Zinedine Zidane is the GOAT french football player"
texts = [text for i in range(1000)]
pdf = pd.DataFrame({'texts': texts})

# COMMAND ----------

df = spark.createDataFrame(pdf)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Parallelize inference with Spark

# COMMAND ----------

import pyspark.sql.types as T
from typing import Iterator
from pyspark.sql.functions import pandas_udf, col

# COMMAND ----------

@pandas_udf(returnType=T.StringType())
def predict_simple(texts: pd.Series) -> pd.Series:
  
  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

  zshot = pipeline(
    task="zero-shot-classification",
    model="MoritzLaurer/deberta-v3-large-zeroshot-v1.1-all-33",
    device=device
  )

  results = []

  for t in texts:
    results.append(zshot(t, candidate_labels=[
              "politics",
              "finance",
              "sports",
              "science and technology",
              "pop culture",
              "breaking news",
          ], multi_label=False)['labels'][0])
  return pd.Series(results)


display(df.select('texts', predict_simple(col('texts'))))

# COMMAND ----------

@pandas_udf(returnType=T.StringType())
def predict_iterator(series: Iterator[pd.Series]) -> Iterator[pd.Series]:
  
  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

  zshot = pipeline(
    task="zero-shot-classification",
    model="MoritzLaurer/deberta-v3-large-zeroshot-v1.1-all-33",
    device=device
  )

  for s in series:
      results = zshot(s.to_list(), candidate_labels=[
                "politics",
                "finance",
                "sports",
                "science and technology",
                "pop culture",
                "breaking news",
            ], multi_label=False)
      output = [result['labels'][0] for result in results]
      yield pd.Series(output)


display(df.select('texts', predict_iterator(col('texts'))))

# COMMAND ----------

@pandas_udf("text string, device string")
def predict_iterator(series: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  
  device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

  zshot = pipeline(
    task="zero-shot-classification",
    model="MoritzLaurer/deberta-v3-large-zeroshot-v1.1-all-33",
    device=device
  )

  for s in series:
      results = zshot(s.to_list(), candidate_labels=[
                "politics",
                "finance",
                "sports",
                "science and technology",
                "pop culture",
                "breaking news",
            ], multi_label=False)
      output = [{"text": result['labels'][0], "device": str(device)} for result in results]
      yield pd.DataFrame(output)


display(df.select('texts', predict_iterator(col('texts'))))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Register model into UC

# COMMAND ----------

import mlflow
mlflow.set_registry_uri('databricks-uc')


# COMMAND ----------

mlflow.__version__

# COMMAND ----------

import pandas as pd
data = pd.DataFrame([], columns = ["text"])

# COMMAND ----------

catalog = "qtoulou_demo"
schema = "nlp_demo"

model_name = f"{catalog}.{schema}.zeroshot_model"
mlflow.transformers.log_model(
  transformers_model=zero_shot_pipeline,
  artifact_path='zs_model',
  registered_model_name=model_name,
  ##signature=signature
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Predict from UC Registered model

# COMMAND ----------

client = mlflow.MlflowClient()

## Get the last version
model_version_infos = client.search_model_versions("name = '%s'" % model_name)
last_v = max([int(model_version_info.version) for model_version_info in model_version_infos])
loaded_model = client.get_model_version(model_name, last_v)


# COMMAND ----------

#artifact_path = loaded_model.__dict__.get('_source').split('/')[-1]
#model_uri = f'runs:/{loaded_model.run_id}/{artifact_path}'
model_uri = loaded_model.source
zeroshot_loaded_model = mlflow.transformers.load_model(model_uri, device=device)

# COMMAND ----------

## https://github.com/mlflow/mlflow/issues/8855
zeroshot_loaded_model = mlflow.transformers.load_model(model_uri, device='cuda')
zeroshot_loaded_model.__dict__

# COMMAND ----------

zeroshot_loaded_model(text, **inference_config)

# COMMAND ----------


