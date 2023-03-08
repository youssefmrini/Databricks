# Databricks notebook source
# MAGIC %md # Training machine learning models on tabular data: an end-to-end example
# MAGIC 
# MAGIC This tutorial covers the following steps:
# MAGIC - Import data from your local machine into the Databricks File System (DBFS)
# MAGIC - Visualize the data using Seaborn and matplotlib
# MAGIC - Run a parallel hyperparameter sweep to train machine learning models on the dataset
# MAGIC - AutoML 
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
# MAGIC   
# MAGIC In this section, you download a dataset from the web and upload it to Databricks File System (DBFS).
# MAGIC 
# MAGIC 1. Navigate to https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/ and download both `winequality-red.csv` and `winequality-white.csv` to your local machine.
# MAGIC 
# MAGIC 1. From this Databricks notebook, select *File* > *Upload Data*, and drag these files to the drag-and-drop target to upload them to the Databricks File System (DBFS). 
# MAGIC 
# MAGIC     **Note**: if you don't have the *File* > *Upload Data* option, you can load the dataset from the Databricks example datasets. Uncomment and run the last two lines in the following cell.
# MAGIC 
# MAGIC 1. Click *Next*. Some auto-generated code to load the data appears. Select *pandas*, and copy the example code. 
# MAGIC 
# MAGIC 1. Create a new cell, then paste in the sample code. It will look similar to the code shown in the following cell. Make these changes:
# MAGIC   - Pass `sep=';'` to `pd.read_csv`
# MAGIC   - Change the variable names from `df1` and `df2` to `white_wine` and `red_wine`, as shown in the following cell.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Libraries
# MAGIC Import the necessary libraries. These libraries are preinstalled on Databricks Runtime for Machine Learning ([AWS](https://docs.databricks.com/runtime/mlruntime.html)|[Azure](https://docs.microsoft.com/azure/databricks/runtime/mlruntime)) clusters and are tuned for compatibility and performance.

# COMMAND ----------

pip install NumPy==1.20

# COMMAND ----------

import mlflow
import numpy as np
import pandas as pd
import sklearn.datasets
import sklearn.metrics
import sklearn.model_selection
import sklearn.ensemble
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score

from hyperopt import fmin, tpe, hp, SparkTrials, Trials, STATUS_OK
from hyperopt.pyll import scope

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Load data
# MAGIC The tutorial uses a dataset describing different wine samples. The [dataset](https://archive.ics.uci.edu/ml/datasets/Wine) is from the UCI Machine Learning Repository and is included in DBFS ([AWS](https://docs.databricks.com/data/databricks-file-system.html)|[Azure](https://docs.microsoft.com/azure/databricks/data/databricks-file-system)).
# MAGIC The goal is to classify red and white wines by their quality. 
# MAGIC 
# MAGIC For more details on uploading and loading from other data sources, see the documentation on working with data ([AWS](https://docs.databricks.com/data/index.html)|[Azure](https://docs.microsoft.com/azure/databricks/data/index)).

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets")

# COMMAND ----------

# If you have the File > Upload Data menu option, follow the instructions in the previous cell to upload the data from your local machine.
# The generated code, including the required edits described in the previous cell, is shown here for reference.

import pandas as pd

# In the following lines, replace <username> with your username.
#white_wine = pd.read_csv("/dbfs/FileStore/shared_uploads/<username>/winequality_white.csv", sep=';')
#red_wine = pd.read_csv("/dbfs/FileStore/shared_uploads/<username>/winequality_red.csv", sep=';')

# If you do not have the File > Upload Data menu option, uncomment and run these lines to load the dataset.
white_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-white.csv", sep=";")
red_wine = pd.read_csv("/dbfs/databricks-datasets/wine-quality/winequality-red.csv", sep=";")

# COMMAND ----------

display(red_wine)

# COMMAND ----------

import uuid
# define a random hash 
uuid_num = uuid.uuid4().hex[:10]
# put anything to distinguish your model, do not use spaces
user_name = 'ym'
database_name = 'youssefmriniml'

# COMMAND ----------


white_wine['is_red'] = 0.0
red_wine['is_red'] = 1.0
data = pd.concat([white_wine, red_wine], axis=0)

# Remove spaces from column names
data.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)
# Let's check the header 
data.head()




# COMMAND ----------

data.shape

# COMMAND ----------

# MAGIC %md ## Visualize data
# MAGIC 
# MAGIC Before training a model, explore the dataset using Seaborn and Matplotlib.

# COMMAND ----------

import seaborn as sns
sns.distplot(data.quality, kde=False)

# COMMAND ----------

# MAGIC %md Looks like quality scores are normally distributed between 3 and 9. 
# MAGIC 
# MAGIC Define a wine as high quality if it has quality >= 7.

# COMMAND ----------

#data_labels = data['quality'] >= 7
#data = data.drop(['quality'], axis=1)
high_quality = (data.quality >= 7).astype(int)
data.quality = high_quality

# COMMAND ----------

display(data)

# COMMAND ----------

# MAGIC %md Box plots are useful in noticing correlations between features and a binary label.

# COMMAND ----------

import matplotlib.pyplot as plt

dims = (3, 4)

f, axes = plt.subplots(dims[0], dims[1], figsize=(25, 15))
axis_i, axis_j = 0, 0
for col in data.columns:
  if col == 'is_red' or col == 'quality':
    continue # Box plots cannot be used on indicator variables
  sns.boxplot(x=high_quality, y=data[col], ax=axes[axis_i, axis_j])
  axis_j += 1
  if axis_j == dims[1]:
    axis_i += 1
    axis_j = 0

# COMMAND ----------

# MAGIC %md In the above box plots, a few variables stand out as good univariate predictors of quality. 
# MAGIC 
# MAGIC - In the alcohol box plot, the median alcohol content of high quality wines is greater than even the 75th quantile of low quality wines. High alcohol content is correlated with quality.
# MAGIC - In the density box plot, low quality wines have a greater density than high quality wines. Density is inversely correlated with quality.

# COMMAND ----------

# MAGIC %md ## Preprocess data
# MAGIC Prior to training a model, check for missing values and split the data into training and validation sets.

# COMMAND ----------

data.isna().any()

# COMMAND ----------


# Split 80/20 train-test
from sklearn.model_selection import train_test_split

train, test = train_test_split(data, random_state=123)
X_train = train.drop(["quality"], axis=1)
X_test = test.drop(["quality"], axis=1)
y_train = train.quality
y_test = test.quality


# COMMAND ----------

# Save test and train into delta 
# write into Spark DF 
spark_df = spark.createDataFrame(data)
# creating if were not a database with a table
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")
spark_df.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').saveAsTable("wine_data")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")
display(spark.sql(f" describe history wine_data"))



# COMMAND ----------

# MAGIC %md ## Part 1. Train a classification model

# COMMAND ----------

# MAGIC %md ### MLflow Tracking
# MAGIC [MLflow tracking](https://www.mlflow.org/docs/latest/tracking.html) allows you to organize your machine learning training code, parameters, and models. 
# MAGIC 
# MAGIC You can enable automatic MLflow tracking by using [*autologging*](https://www.mlflow.org/docs/latest/tracking.html#automatic-logging).

# COMMAND ----------

import mlflow

mlflow.autolog()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The following libraries support autologging:
# MAGIC <ul>
# MAGIC   <li> Scikit-learn </li>
# MAGIC   <li>TensorFlow </li>
# MAGIC   <li>Keras </li>
# MAGIC   <li>Gluon </li>
# MAGIC   <li>XGBoost</li>
# MAGIC   <li>LightGBM</li>
# MAGIC   <li>Statsmodels</li>
# MAGIC   <li>Spark </li>
# MAGIC   <li>Fastai</li>
# MAGIC   <li>Pytorch </li>
# MAGIC   <li>FBProphet </li>
# MAGIC   <li> PMDARIMA </li>
# MAGIC </ul>

# COMMAND ----------

# MAGIC %md
# MAGIC Next, train a classifier within the context of an MLflow run, which automatically logs the trained model and many associated metrics and parameters. 
# MAGIC 
# MAGIC You can supplement the logging with additional metrics such as the model's AUC score on the test dataset.

# COMMAND ----------

with mlflow.start_run(run_name=f'RandomForestClassifier{user_name}') as run:
  n_estimators = 5
  model = RandomForestClassifier( n_estimators=n_estimators,random_state=np.random.RandomState(123))
   
  # Models, parameters, and training metrics are tracked automatically
  model.fit(X_train, y_train)

  predicted_probs = model.predict_proba(X_test)[:,1]
  roc_auc = roc_auc_score(y_test, predicted_probs)
  
  # The AUC score on test data is not automatically logged, so log it manually
  mlflow.log_metric("test_auc", roc_auc)
  
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <b>mlflow.log_param()</b> logs a single key-value param in the currently active run.
# MAGIC 
# MAGIC <b>mlflow.log_metric() </b> logs a single key-value metric. The value must always be a number.
# MAGIC 
# MAGIC <b> mlflow.set_tag() </b> sets a single key-value tag in the currently active run.

# COMMAND ----------

# MAGIC %md
# MAGIC If you aren't happy with the performance of this model, train another model with different hyperparameters.

# COMMAND ----------

with mlflow.start_run(run_name=f'RandomForestClassifier{user_name}') as run:
  n_estimators = 10
 
  model = RandomForestClassifier( n_estimators=n_estimators,random_state=np.random.RandomState(123))
  model.fit(X_train, y_train)

  predicted_probs = model.predict_proba(X_test)[:,1]
  roc_auc = sklearn.metrics.roc_auc_score(y_test, predicted_probs)
  mlflow.log_metric("test_auc", roc_auc)
  mlflow.log_param("delta",22)
  mlflow.set_tag("team","Prima")
  print("Test AUC of: {}".format(roc_auc))

# COMMAND ----------

# MAGIC %md #### Register the model in MLflow Model Registry
# MAGIC 
# MAGIC By registering this model in Model Registry, you can easily reference the model from anywhere within Databricks.
# MAGIC 
# MAGIC The following section shows how to do this programmatically, but you can also register a model using the UI. See "Create or register a model using the UI" ([AWS](https://docs.databricks.com/applications/machine-learning/manage-model-lifecycle/index.html#create-or-register-a-model-using-the-ui)|[Azure](https://docs.microsoft.com/azure/databricks/applications/machine-learning/manage-model-lifecycle/index#create-or-register-a-model-using-the-ui)|[GCP](https://docs.gcp.databricks.com/applications/machine-learning/manage-model-lifecycle/index.html#create-or-register-a-model-using-the-ui)).

# COMMAND ----------

run_id = mlflow.search_runs(filter_string=f'tags.mlflow.runName = "RandomForestClassifier{user_name}"' ,order_by=["metrics.test_auc ASC"]).iloc[0].run_id
print(run_id)

# COMMAND ----------

model_name = f"wine_quality_{user_name}_{uuid_num}"

model_version=mlflow.register_model(f"runs:/{run_id}/model", model_name)


# COMMAND ----------

# MAGIC %md You should now see the model in the Models page. To display the Models page, click the Models icon in the left sidebar. 
# MAGIC 
# MAGIC Next, transition this model to production and load it into this notebook from Model Registry.

# COMMAND ----------

print('Your registered model name is : {}'.format(model_name))
 

# COMMAND ----------

print(model_version.version)

# COMMAND ----------

#Transition an mlflow model's stage
client = mlflow.tracking.MlflowClient()

client.transition_model_version_stage(
    name=model_name,
    version=model_version.version,
    stage="Production"
)


# COMMAND ----------

#Update the model_version
client.update_model_version(
    name=model_name,
    version=model_version.version,
    description="This model version is a scikit-learn random forest"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Load models
# MAGIC You can also access the results for a specific run using the MLflow API. The code in the following cell illustrates how to load the model trained in a given MLflow run and use it to make predictions. You can also find code snippets for loading specific models on the MLflow run page ([AWS](https://docs.databricks.com/applications/mlflow/tracking.html#view-notebook-experiment)|[Azure](https://docs.microsoft.com/azure/databricks/applications/mlflow/tracking#view-notebook-experiment)).

# COMMAND ----------

model = mlflow.pyfunc.load_model(f"models:/{model_name}/production")
tables=pd.DataFrame(data)
preds=model.predict(pd.DataFrame(data))
tables.insert(13,"predictions",preds)
tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Spark dataframe

# COMMAND ----------

import mlflow
logged_model = (f"models:/{model_name}/1")

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model, result_type='double')

# Predict on a Spark DataFrame.

df=spark_df.select('fixed_acidity', 'volatile_acidity', 'citric_acid', 'residual_sugar', 'chlorides', 'free_sulfur_dioxide', 'total_sulfur_dioxide', 'density', 'pH', 'sulphates', 'alcohol', 'is_red')	
columns = list(df.columns)
display(df.withColumn('predictions', loaded_model(*columns)))

# COMMAND ----------

# Delete a registered model along with all its versions
client = mlflow.tracking.MlflowClient()
client.delete_registered_model(name=model_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## AutoML 

# COMMAND ----------

# read the data
spark_df = spark.read.format("delta").table(f'{database_name}.wine_data')
train_df, test_df = spark_df.randomSplit([.8, .2], seed=42)

# COMMAND ----------

from databricks import automl
summary = automl.classify(train_df, target_col="quality", primary_metric="roc_auc", timeout_minutes=5)

# COMMAND ----------

print(summary.best_trial)

# COMMAND ----------

model = mlflow.pyfunc.load_model(f"runs:/{summary.best_trial.mlflow_run_id}/model")
tables=pd.DataFrame(data)
preds=model.predict(pd.DataFrame(data))
tables.insert(13,"predictionss",preds)
tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Serving

# COMMAND ----------

import os
os.environ["DATABRICKS_TOKEN"] = "dapiddd91d1a7482c777aaf2ab44b62d7b54" # demo_workshop


# COMMAND ----------

# MAGIC %sh
# MAGIC curl \
# MAGIC   -u token:dapide385b20ee2b3defed7acb5e0a277c61 \
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
# MAGIC https://cse2.cloud.databricks.com/model/wine_quality_ym_e518dc7bbc/Production/invocations
