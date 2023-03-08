# Databricks notebook source
# MAGIC %md # Training machine learning models on tabular data: an end-to-end example
# MAGIC 
# MAGIC This tutorial covers the following steps:
# MAGIC - Explore the results of the hyperparameter sweep with MLflow
# MAGIC - Register the best performing model in MLflow
# MAGIC - Apply the registered model to another dataset using a Spark UDF
# MAGIC - Set up model serving for low-latency requests
# MAGIC 
# MAGIC In this example, you build a model to predict the quality of Portugese "Vinho Verde" wine based on the wine's physicochemical properties. 
# MAGIC 
# MAGIC The example uses a dataset from the UCI Machine Learning Repository, presented in [*Modeling wine preferences by data mining from physicochemical properties*](https://www.sciencedirect.com/science/article/pii/S0167923609001377?via%3Dihub) [Cortez et al., 2009].
# MAGIC 
# MAGIC ## Requirements
# MAGIC This notebook requires Databricks Runtime for Machine Learning.  
# MAGIC If you are using Databricks Runtime 7.3 LTS ML or below, you must update the CloudPickle library. To do that, uncomment and run the `%pip install` command in Cmd 2. 

# COMMAND ----------

# MAGIC %md ## Import data
# MAGIC   Data was imported in Part1 please go back there
# MAGIC   

# COMMAND ----------

# HERE PLEASE MODIFY THIS 
model_name = "wine_quality_ym_9b77a39f54"
# put anything to distinguish your model, do not use spaces
user_name = 'yms' # use the same that was defined in Part1
database_name = 'youssefmriniml' # use the same that was defined in Part1
model_version_old = 1 # check the model that was put in production 

# COMMAND ----------

import pandas as pd
spark_df = spark.read.format('delta').table(f"{database_name}.wine_data")
data  = spark_df.toPandas()

# COMMAND ----------

from sklearn.model_selection import train_test_split
train, test = train_test_split(data, random_state=123)
X_train = train.drop(["quality"], axis=1)
X_test = test.drop(["quality"], axis=1)
y_train = train.quality
y_test = test.quality

# COMMAND ----------

# MAGIC %md  #### Use MLflow to view the results
# MAGIC Open up the Experiment Runs sidebar to see the MLflow runs. Click on Date next to the down arrow to display a menu, and select 'auc' to display the runs sorted by the auc metric. The highest auc value is 0.91. You beat the baseline!
# MAGIC 
# MAGIC MLflow tracks the parameters and performance metrics of each run. Click the External Link icon <img src="https://docs.databricks.com/_static/images/icons/external-link.png"/> at the top of the Experiment Runs sidebar to navigate to the MLflow Runs Table.

# COMMAND ----------

from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope
from math import exp
import numpy as np
import xgboost as xgb

import mlflow
import mlflow.pyfunc
import mlflow.sklearn
import numpy as np
import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from mlflow.models.signature import infer_signature
from mlflow.utils.environment import _mlflow_conda_env
import cloudpickle
import time

search_space = {
  'max_depth': scope.int(hp.quniform('max_depth', 4, 100, 1)),
  'learning_rate': hp.loguniform('learning_rate', -3, 0),
  'reg_alpha': hp.loguniform('reg_alpha', -5, -1),
  'reg_lambda': hp.loguniform('reg_lambda', -6, -1),
  'min_child_weight': hp.loguniform('min_child_weight', -1, 3),
  'seed': 123, # Set a seed for deterministic training
}


def train_model(params):
  # With MLflow autologging, hyperparameters and the trained model are automatically logged to MLflow.
  mlflow.xgboost.autolog()
  with mlflow.start_run(nested=True):
    train = xgb.DMatrix(data=X_train, label=y_train)
    test = xgb.DMatrix(data=X_test, label=y_test)
    # Pass in the test set so xgb can track an evaluation metric. XGBoost terminates training when the evaluation metric
    # is no longer improving.
    booster = xgb.train(params=params, dtrain=train, num_boost_round=1000,\
                        evals=[(test, "test")], early_stopping_rounds=50)
    predictions_test = booster.predict(test)
    auc_score = roc_auc_score(y_test, predictions_test)
    mlflow.log_metric('auc', auc_score)

    signature = infer_signature(X_train, booster.predict(train))
    mlflow.xgboost.log_model(booster, "model", signature=signature)
    
    # Set the loss to -1*auc_score so fmin maximizes the auc_score
    return {'status': STATUS_OK, 'loss': -1*auc_score, 'booster': booster.attributes()}
  # Greater parallelism will lead to speedups, but a less optimal hyperparameter sweep. 
  
# A reasonable value for parallelism is the square root of max_evals.
spark_trials = SparkTrials(parallelism=2)

# Run fmin within an MLflow run context so that each hyperparameter configuration is logged as a child run of a parent
# run called "xgboost_models" .
with mlflow.start_run(run_name=f'xgboost_models_{user_name}'):
  best_params = fmin(
    fn=train_model, 
    space=search_space, 
    algo=tpe.suggest, 
    max_evals=8, # to make this faster choose less trials to run 
    trials=spark_trials, 
  )

# COMMAND ----------


best_run = mlflow.search_runs(order_by=['metrics.auc DESC']).iloc[0]

print(best_run)

# COMMAND ----------

print('Best Run')
print('AUC: {}'.format(best_run["metrics.auc"]))
print('Num Estimators: {}'.format(best_run["params.max_depth"]))
print('learning_rate: {}'.format(best_run["params.learning_rate"]))
print('reg_alpha: {}'.format(best_run["params.reg_alpha"]))
print('reg_lambda: {}'.format(best_run["params.reg_lambda"]))
print('min_child_weight: {}'.format(best_run["params.min_child_weight"]))

print('Max Depth: {}'.format(best_run["params.max_depth"]))
print('Learning Rate: {}'.format(best_run["params.learning_rate"]))

best_model_pyfunc = mlflow.pyfunc.load_model(
  'runs:/{run_id}/model'.format(
    run_id=best_run.run_id
  )
)
print(best_run.run_id)

# COMMAND ----------

# MAGIC %md Now investigate how the hyperparameter choice correlates with AUC. Click the "+" icon to expand the parent run, then select all runs except the parent, and click "Compare". Select the Parallel Coordinates Plot.
# MAGIC 
# MAGIC The Parallel Coordinates Plot is useful in understanding the impact of parameters on a metric. You can drag the pink slider bar at the upper right corner of the plot to highlight a subset of AUC values and the corresponding parameter values. The plot below highlights the highest AUC values:
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_static/images/mlflow/end-to-end-example/parallel-coordinates-plot.png"/>
# MAGIC 
# MAGIC Notice that all of the top performing runs have a low value for reg_lambda and learning_rate. 
# MAGIC 
# MAGIC You could run another hyperparameter sweep to explore even lower values for these parameters. For simplicity, that step is not included in this example.

# COMMAND ----------

# MAGIC %md 
# MAGIC You used MLflow to log the model produced by each hyperparameter configuration. The following code finds the best performing run and saves the model to Model Registry.

# COMMAND ----------

# MAGIC %md #### Update the production `wine_quality` model in MLflow Model Registry
# MAGIC 
# MAGIC Earlier, you saved the baseline model to Model Registry with the name `wine_quality`. Now that you have a created a more accurate model, update `wine_quality`.

# COMMAND ----------

from mlflow.tracking import MlflowClient
client = MlflowClient()

new_model_version = mlflow.register_model(f"runs:/{best_run.run_id}/model", model_name)
# Registering the model takes a few seconds, so add a small delay
time.sleep(15)

# COMMAND ----------

# Archive the old model version
client.transition_model_version_stage(
  name=model_name,
  #version=model_version.version,
  version = model_version_old,
  stage="Archived"
)

# Promote the new model version to Production
client.transition_model_version_stage(
  name=model_name,
  version=new_model_version.version,
  stage="Production"
)

# COMMAND ----------

# MAGIC %md Clients that call load_model now receive the new model.

# COMMAND ----------

# This code is the same as the last block of "Building a Baseline Model". No change is required for clients to get the new model!
model = mlflow.pyfunc.load_model(f"models:/{model_name}/production")
print(f'AUC: {roc_auc_score(y_test, model.predict(X_test))}')

# COMMAND ----------

# MAGIC %md ##Batch inference
# MAGIC 
# MAGIC There are many scenarios where you might want to evaluate a model on a corpus of new data. For example, you may have a fresh batch of data, or may need to compare the performance of two models on the same corpus of data.
# MAGIC 
# MAGIC The following code evaluates the model on data stored in a Delta table, using Spark to run the computation in parallel.

# COMMAND ----------

# To simulate a new corpus of data, save the existing X_train data to a Delta table. 
# In the real world, this would be a new batch of data.
spark_df_train = spark.createDataFrame(X_train)
# Replace <username> with your username before running this cell.
table_path_train = f"dbfs:/{user_name}/wine_data_train"
# Delete the contents of this path in case this cell has already been run
dbutils.fs.rm(table_path_train, True)
spark_df_train.write.format("delta").save(table_path_train)

# COMMAND ----------

import mlflow.pyfunc
apply_model_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{model_name}/production")
# Read the "new data" from Delta
new_data = spark.read.format("delta").load(table_path_train)
display(new_data)

# COMMAND ----------

from pyspark.sql.functions import struct

# Apply the model to the new data
udf_inputs = struct(*(X_train.columns.tolist()))

new_data = new_data.withColumn(
  "prediction",
  apply_model_udf(udf_inputs)
)
display(new_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model serving
# MAGIC 
# MAGIC To productionize the model for low latency predictions, use MLflow Model Serving ([AWS](https://docs.databricks.com/applications/mlflow/model-serving.html)|[Azure](https://docs.microsoft.com/azure/databricks/applications/mlflow/model-serving)|[GCP](https://docs.gcp.databricks.com/applications/mlflow/model-serving.html)) to deploy the model to an endpoint.
# MAGIC 
# MAGIC The following code illustrates how to issue requests using a REST API to get predictions from the deployed model.

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC You need a Databricks token to issue requests to your model endpoint. You can generate a token from the User Settings page (click Settings in the left sidebar). Copy the token into the next cell.

# COMMAND ----------

import os
os.environ["DATABRICKS_TOKEN"] = "" # demo_workshop


# COMMAND ----------

# MAGIC %md
# MAGIC Click **Models** in the left sidebar and navigate to the registered wine model. Click the serving tab, and then click **Enable Serving**.
# MAGIC 
# MAGIC Then, under **Call The Model**, click the **Python** button to display a Python code snippet to issue requests. Copy the code into this notebook. It should look similar to the code in the next cell. 
# MAGIC 
# MAGIC You can use the token to make these requests from outside Databricks notebooks as well.

# COMMAND ----------

# Replace with code snippet from the model serving page
import os
import requests
import numpy as np
import pandas as pd

def create_tf_serving_json(data):
  return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(dataset):
  url = f'https://cse2.cloud.databricks.com/model/wine_quality_ym_54f2ba68e5/1/invocations'
  headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}'}
  data_json = dataset.to_dict(orient='split') if isinstance(dataset, pd.DataFrame) else create_tf_serving_json(dataset)
  response = requests.request(method='POST', headers=headers, url=url, json=data_json)
  if response.status_code != 200:
    raise Exception(f'Request failed with status {response.status_code}, {response.text}')
  return response.json()

# COMMAND ----------

# Model serving is designed for low-latency predictions on smaller batches of data
num_predictions = 5
served_predictions = score_model(X_test[:num_predictions])
model_evaluations = model.predict(X_test[:num_predictions])
# Compare the results from the deployed model and the trained model
pd.DataFrame({
  "Model Prediction": model_evaluations,
  "Served Model Prediction": served_predictions,
})

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Another way of serving with curl

# COMMAND ----------

# MAGIC %sh
# MAGIC curl \
# MAGIC   -u token:$DATABRICKS_TOKEN \
# MAGIC   -X POST \
# MAGIC   -H "Content-Type: application/json" \
# MAGIC   -d '[{
# MAGIC         "fixed_acidity": 7,
# MAGIC         "volatile_acidity": 0.27,
# MAGIC         "citric_acid": 0.36,
# MAGIC         "residual_sugar": 0.8,
# MAGIC         "chlorides": 0.346,
# MAGIC         "free_sulfur_dioxide": 3,
# MAGIC         "total_sulfur_dioxide": 64,
# MAGIC         "density": 0.9892,
# MAGIC         "pH": 0.36,
# MAGIC         "sulphates": 0.27,
# MAGIC         "alcohol": 13.9,
# MAGIC         "is_red": 0 }]' \
# MAGIC   https://cse2.cloud.databricks.com/model/wine_quality_ym_5ee29aa1cb/1/invocations
