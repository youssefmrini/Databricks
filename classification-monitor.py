# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse monitoring example notebook: InferenceLog classification analysis
# MAGIC
# MAGIC **User requirements**
# MAGIC - You must have access to run commands on a cluster with access to Unity Catalog.
# MAGIC - You must have `USE CATALOG` privilege on at least one catalog, and you must have `USE SCHEMA` privileges on at least one schema. This notebook creates tables in the `main.default` schema. If you do not have the required privileges on the `main.default` schema, you must edit the notebook to change the default catalog and schema to ones that you do have privileges on.
# MAGIC
# MAGIC **System requirements:**
# MAGIC - Unity-Catalog enabled workspace.
# MAGIC - Databricks Runtime 12.2 LTS ML or above.
# MAGIC - Single-User/Assigned cluster (Preferred if you want to run the notebook as is to train the model and create the tables)
# MAGIC
# MAGIC This notebook illustrates how to train and deploy a classification model and monitor its corresponding batch inference table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC * Verify cluster configuration
# MAGIC * Install Python client
# MAGIC * Define catalog, schema, model and table names

# COMMAND ----------

# DBTITLE 1,Install Lakehouse Monitoring client wheel
# MAGIC %pip install "https://ml-team-public-read.s3.amazonaws.com/wheels/data-monitoring/a4050ef7-b183-47a1-a145-e614628e3146/databricks_lakehouse_monitoring-0.4.6-py3-none-any.whl"

# COMMAND ----------

# This step is necessary to reset the environment with our newly installed wheel.
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC --create catalog ml_youssef;
# MAGIC use catalog ml_youssef;
# MAGIC create schema demo;
# MAGIC use demo;

# COMMAND ----------

# DBTITLE 1,Specify catalog and schema to use
# You must have `USE CATALOG` privileges on the catalog, and you must have `USE SCHEMA` privileges on the schema.
# If necessary, change the catalog and schema name here.

CATALOG = "ml_youssef"
SCHEMA = "demo"

# COMMAND ----------

username = spark.sql("SELECT current_user()").first()["current_user()"]
username_prefixes = username.split("@")[0].split(".")

# COMMAND ----------

unique_suffix = "_".join([username_prefixes[0], username_prefixes[1][0:2]])
TABLE_NAME = f"{CATALOG}.{SCHEMA}.adult_census_inferencelogs_{unique_suffix}"
BASELINE_TABLE = f"{CATALOG}.{SCHEMA}.adult_census_baseline_{unique_suffix}"
MODEL_NAME = f"adult_census_{unique_suffix}" # Name of (registered) model in mlflow registry
TIMESTAMP_COL = "timestamp"
MODEL_ID_COL = "model_id" # Name of column to use as model identifier (here we'll use the model_name+version)
PREDICTION_COL = "prediction"  # What to name predictions in the generated tables
LABEL_COL = "income" # Name of ground-truth labels column
ID_COL = "ID" # [OPTIONAL] only used for joining labels

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
spark.sql(f"DROP TABLE IF EXISTS {BASELINE_TABLE}")

# COMMAND ----------

from mlflow.client import MlflowClient

def cleanup_registered_model(registry_model_name):
  """
  Utilty function to delete a registered model in MLflow model registry.
  To delete a model in the model registry, all model versions must first be archived.
  This function 
  (i) first archives all versions of a model in the registry
  (ii) then deletes the model 
  
  :param registry_model_name: (str) Name of model in MLflow Model Registry
  """

  filter_string = f'name="{registry_model_name}"'
  model_exist = client.search_registered_models(filter_string=filter_string)

  if model_exist:
    model_versions = client.search_model_versions(filter_string=filter_string)
    print(f"Deleting model named {registry_model_name}...")
    if len(model_versions) > 0:
      print(f"Purging {len(model_versions)} versions...")
      # Move any versions of the model to Archived
      for model_version in model_versions:
        try:
          model_version = client.transition_model_version_stage(
            name=model_version.name,
            version=model_version.version,
            stage="Archived",
          )
        except mlflow.exceptions.RestException:
          pass
    client.delete_registered_model(registry_model_name)
  else:
    print(f"No registered model named {registry_model_name} to delete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Background
# MAGIC The following are required to create an inference log monitor:
# MAGIC - A Delta table in Unity Catalog that you own.
# MAGIC - The data can be batch scored data or inference logs. The following columns are required:  
# MAGIC   - `timestamp` (TimeStamp): Used for windowing and aggregation when calculating metrics
# MAGIC   - `model_id` (String): Model version/id used for each prediction.
# MAGIC   - `prediction` (String): Value predicted by the model.
# MAGIC   
# MAGIC - The following column is optional:  
# MAGIC   - `label` (String): Ground truth label.
# MAGIC
# MAGIC You can also provide an optional baseline table to track performance changes in the model and drifts in the statistical characteristics of features. 
# MAGIC - To track performance changes in the model, consider using the test or validation set.
# MAGIC - To track drifts in feature distributions, consider using the training set or the associated feature tables. 
# MAGIC - The baseline table must use the same column names as the monitored table, and must also have a `model_version` column.
# MAGIC
# MAGIC Databricks recommends enabling Delta's Change-Data-Feed ([AWS](https://docs.databricks.com/delta/delta-change-data-feed.html#enable-change-data-feed)|[Azure](https://learn.microsoft.com/azure/databricks/delta/delta-change-data-feed#enable-change-data-feed)) table property for better metric computation performance for all monitored tables, including the baseline table. This notebook shows how to enable Change Data Feed when you create the Delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Journey
# MAGIC 1. Create Delta table: Read raw input and features data and create training and inference sets.
# MAGIC 2. Train a model, register the model the MLflow Model Registry.
# MAGIC 3. Generate predictions on test set and create the baseline table.
# MAGIC 4. Generate predictions on `scoring_df1`. This is the inference table.
# MAGIC 5. Create the monitor on the inference table and analyse profile/drift metrics and fairness and bias metrics.
# MAGIC 6. Simulate drifts in 3 relevant features, `scoring_df2` and generate/materialize predictions.
# MAGIC 7. Add/Join ground-truth labels to monitoring table and refresh monitor.
# MAGIC 8. [Optional] Calculate custom metrics.
# MAGIC 9. [Optional] Delete the monitor.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read dataset and prepare data
# MAGIC Dataset used for this example: [UCI's Adult Census](https://archive.ics.uci.edu/dataset/2/adult)
# MAGIC * Add a dummy identifer
# MAGIC * Clean and standardize missing values

# COMMAND ----------

from pyspark.sql import functions as F

schema = """`age` DOUBLE,
`workclass` STRING,
`fnlwgt` DOUBLE,
`education` STRING,
`education_num` DOUBLE,
`marital_status` STRING,
`occupation` STRING,
`relationship` STRING,
`race` STRING,
`sex` STRING,
`capital_gain` DOUBLE,
`capital_loss` DOUBLE,
`hours_per_week` DOUBLE,
`native_country` STRING,
`income` STRING"""

# Read data and add a unique id column
raw_df = (
  spark.read.csv("/databricks-datasets/adult/adult.data", schema=schema)
  .withColumn(ID_COL, F.expr("uuid()"))
)

display(raw_df)

# COMMAND ----------

from pyspark.sql import types as T


# Trim all strings and replace "?" with None/Null value
clean_df = raw_df
for field in raw_df.schema.fields:
    if isinstance(field.dataType, T.StringType):
        clean_df = clean_df.withColumn(field.name, F.trim(F.col(field.name)))

clean_df = clean_df.na.replace("?", None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Split data
# MAGIC Split data into a training set, baseline test table, and inference table. 
# MAGIC - The baseline test data will serve as the table with reference feature distributions.
# MAGIC - The inference table will then be split into two dataframes, `scoring_df1` and `scoring_df2`: they will function as new incoming batches for scoring. We will further simulate drifts on the `scoring_df`(s).

# COMMAND ----------

train_df, baseline_test_df, inference_df = clean_df.randomSplit(weights=[0.6, 0.2, 0.2], seed=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Train a random forest model

# COMMAND ----------

import mlflow
import sklearn

from datetime import timedelta, datetime
from mlflow.tracking import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

client = MlflowClient()

# COMMAND ----------

# clean up existing model [optional]
cleanup_registered_model(MODEL_NAME)

# COMMAND ----------

# Define the training datasets
X_train = train_df.drop(ID_COL, LABEL_COL).toPandas()
Y_train = train_df.select(LABEL_COL).toPandas().values.ravel()

display(Y_train)

# COMMAND ----------

# Define categorical and numerical preprocessors
categorical_cols = [col for col in X_train if X_train[col].dtype == "object"]
numerical_cols = [col for col in X_train if X_train[col].dtype != "object"]
cat_pipeline = Pipeline(steps=[("imputer", SimpleImputer(strategy='most_frequent')),("one_hot_encoder", OneHotEncoder(handle_unknown="ignore"))])
num_pipeline = Pipeline(steps=[("imputer", SimpleImputer(strategy='median'))])                        
preprocessor = ColumnTransformer([("cat", cat_pipeline, categorical_cols), ("num", num_pipeline, numerical_cols)], remainder="passthrough", sparse_threshold=0)

# Define the model
skrf_classifier = RandomForestClassifier(
  max_depth=5,
  max_features=0.5,
  min_samples_leaf=0.1,
  min_samples_split=0.15,
  n_estimators=36,
  random_state=42,
)

model = Pipeline([
  ("preprocessor", preprocessor),
  ("classifier", skrf_classifier),
])

# Enable automatic logging of input samples, metrics, parameters, and models
mlflow.sklearn.autolog(log_input_examples=True, silent=True, registered_model_name=MODEL_NAME)

with mlflow.start_run(run_name="random_forest_classifier") as mlflow_run:
  model.fit(X_train, Y_train)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create baseline table 
# MAGIC
# MAGIC For information about how to select a baseline table, see the Lakehouse Monitoring documentation  ([AWS](https://docs.databricks.com/en/lakehouse-monitoring/index.html#primary-table-and-baseline-table)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/index#primary-table-and-baseline-table)).

# COMMAND ----------

model_version_infos = client.search_model_versions("name = '%s'" % MODEL_NAME)
new_model_version = max([model_version_info.version for model_version_info in model_version_infos])
model_uri = f"models:/{MODEL_NAME}/{new_model_version}"
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="string")
features = list(X_train.columns)

# COMMAND ----------

features

# COMMAND ----------

# Add prediction and model version column
baseline_test_df_with_pred = (baseline_test_df
  .withColumn(PREDICTION_COL, loaded_model(*features))
  .withColumn(MODEL_ID_COL, F.lit(new_model_version))
)

display(baseline_test_df_with_pred)

# COMMAND ----------

# DBTITLE 1,Write table with CDF enabled
(baseline_test_df_with_pred
  .write
  .format("delta")
  .mode("append")
  .option("overwriteSchema",True)
  .option("delta.enableChangeDataFeed", "true")
  .saveAsTable(BASELINE_TABLE)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generate predictions on incoming scoring data
# MAGIC
# MAGIC ### Example pre-processing step
# MAGIC - Extract ground-truth labels (in practice, labels might arrive later)
# MAGIC - Split into two batches

# COMMAND ----------

test_labels_df = inference_df.select(ID_COL, LABEL_COL)
scoring_df1, scoring_df2 = inference_df.randomSplit(weights=[0.5, 0.5], seed=42)

# COMMAND ----------

# Simulate timestamp(s) if they don't exist
timestamp1 = (datetime.now() + timedelta(1)).timestamp()

pred_df1 = (scoring_df1
  .withColumn(TIMESTAMP_COL, F.lit(timestamp1).cast("timestamp")) 
  .withColumn(PREDICTION_COL, loaded_model(*features))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Write scoring data with predictions out 
# MAGIC * Add `model_version` column and write to the table that we will attach a monitor to
# MAGIC * Add ground-truth `label_col` column with empty/NaN values
# MAGIC
# MAGIC Set `mergeSchema` to `True` to enable appending dataframes without label column available

# COMMAND ----------

(pred_df1
  .withColumn(MODEL_ID_COL, F.lit(new_model_version))
  #.withColumn(LABEL_COL, F.lit(None).cast("string"))
  .write.format("delta").mode("overwrite") 
  .option("mergeSchema",True) 
  .option("delta.enableChangeDataFeed", "true") 
  .saveAsTable(TABLE_NAME)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create the monitor
# MAGIC Use `InferenceLog` type analysis.
# MAGIC
# MAGIC **Make sure to drop any column that you don't want to track or which doesn't make sense from a business or use-case perspective**, otherwise create a VIEW with only columns of interest and monitor it.

# COMMAND ----------

import databricks.lakehouse_monitoring as lm

# COMMAND ----------

help(lm.create_monitor)

# COMMAND ----------

# ML problem type, either "classification" or "regression"
PROBLEM_TYPE = "classification"

# Window sizes to analyze data over
GRANULARITIES = ["1 day"]                       

# Optional parameters
SLICING_EXPRS = ["age<25", "age>60", "sex='Male'", "race='White'"]   # Expressions to slice data with

# COMMAND ----------

# DBTITLE 1,Create Monitor
print(f"Creating monitor for {TABLE_NAME}")

info = lm.create_monitor(
  table_name=TABLE_NAME,
  profile_type=lm.InferenceLog(
    timestamp_col=TIMESTAMP_COL,
    granularities=GRANULARITIES,
    model_id_col=MODEL_ID_COL, # Model version number 
    prediction_col=PREDICTION_COL,
    problem_type=PROBLEM_TYPE,
    label_col=LABEL_COL # Optional
  ),
  baseline_table_name=BASELINE_TABLE,
  slicing_exprs=SLICING_EXPRS,
  output_schema_name=f"{CATALOG}.{SCHEMA}"
)

# COMMAND ----------

import time


# Wait for monitor to be created
while info.status == lm.MonitorStatus.PENDING:
  info = lm.get_monitor(table_name=TABLE_NAME)
  time.sleep(10)

assert info.status == lm.MonitorStatus.ACTIVE, "Error creating monitor"

# COMMAND ----------

# A metric refresh will automatically be triggered on creation
refreshes = lm.list_refreshes(table_name=TABLE_NAME)
assert(len(refreshes) > 0)

run_info = refreshes[0]
while run_info.state in (lm.RefreshState.PENDING, lm.RefreshState.RUNNING):
  run_info = lm.get_refresh(table_name=TABLE_NAME, refresh_id=run_info.refresh_id)
  time.sleep(30)

assert run_info.state == lm.RefreshState.SUCCESS, "Monitor refresh failed"

# COMMAND ----------

# MAGIC %md
# MAGIC Click the highlighted Dashboard link in the cell output to open the dashboard. You can also navigate to the dashboard from the Catalog Explorer UI.

# COMMAND ----------

lm.get_monitor(table_name=TABLE_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Inspect the metrics tables
# MAGIC
# MAGIC By default, the metrics tables are saved in the default database.  
# MAGIC
# MAGIC The `create_monitor` call created two new tables: the profile metrics table and the drift metrics table. 
# MAGIC
# MAGIC These two tables record the outputs of analysis jobs. The tables use the same name as the primary table to be monitored, with the suffixes `_profile_metrics` and `_drift_metrics`.

# COMMAND ----------

# MAGIC %md ### Orientation to the profile metrics table
# MAGIC
# MAGIC The profile metrics table has the suffix `_profile_metrics`. For a list of statistics that are shown in the table, see the documentation ([AWS](https://docs.databricks.com/lakehouse-monitoring/monitor-output.html#profile-metrics-table)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/monitor-output#profile-metrics-table)).
# MAGIC
# MAGIC - For every column in the primary table, the profile table shows summary statistics for the baseline table and for the primary table. The column `log_type` shows `INPUT` to indicate statistics for the primary table, and `BASELINE` to indicate statistics for the baseline table. The column from the primary table is identified in the column `column_name`.
# MAGIC - For `TimeSeries` type analysis, the `granularity` column shows the granularity corresponding to the row. For baseline table statistics, the `granularity` column shows `null`.
# MAGIC - The table shows statistics for each value of each slice key in each time window, and for the table as whole. Statistics for the table as a whole are indicated by `slice_key` = `slice_value` = `null`.
# MAGIC - In the primary table, the `window` column shows the time window corresponding to that row. For baseline table statistics, the `window` column shows `null`.  
# MAGIC - Some statistics are calculated based on the table as a whole, not on a single column. In the column `column_name`, these statistics are identified by `:table`.

# COMMAND ----------

# Display profile metrics table
profile_table = f"{TABLE_NAME}_profile_metrics"
profile_df = spark.sql(f"SELECT * FROM {profile_table}")
display(profile_df)

# COMMAND ----------

# MAGIC %md ### Orientation to the drift metrics table
# MAGIC
# MAGIC The drift metrics table has the suffix `_drift_metrics`. For a list of statistics that are shown in the table, see the documentation ([AWS](https://docs.databricks.com/lakehouse-monitoring/monitor-output.html#drift-metrics-table)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/monitor-output#drift-metrics-table)).
# MAGIC
# MAGIC - For every column in the primary table, the drift table shows a set of metrics that compare the current values in the table to the values at the time of the previous analysis run and to the baseline table. The column `drift_type` shows `BASELINE` to indicate drift relative to the baseline table, and `CONSECUTIVE` to indicate drift relative to a previous time window. As in the profile table, the column from the primary table is identified in the column `column_name`.
# MAGIC   - At this point, because this is the first run of this monitor, there is no previous window to compare to. So there are no rows where `drift_type` is `CONSECUTIVE`. 
# MAGIC - For `TimeSeries` type analysis, the `granularity` column shows the granularity corresponding to that row.
# MAGIC - The table shows statistics for each value of each slice key in each time window, and for the table as whole. Statistics for the table as a whole are indicated by `slice_key` = `slice_value` = `null`.
# MAGIC - The `window` column shows the the time window corresponding to that row. The `window_cmp` column shows the comparison window. If the comparison is to the baseline table, `window_cmp` is `null`.  
# MAGIC - Some statistics are calculated based on the table as a whole, not on a single column. In the column `column_name`, these statistics are identified by `:table`.

# COMMAND ----------

# Display the drift metrics table
drift_table = f"{TABLE_NAME}_drift_metrics"
display(spark.sql(f"SELECT * FROM {drift_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Look at fairness and bias metrics
# MAGIC Fairness and bias metrics are calculated for boolean type slices that were defined. The group defined by `slice_value=true` is considered the protected group ([AWS](https://docs.databricks.com/en/lakehouse-monitoring/fairness-bias.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-monitoring/fairness-bias)).

# COMMAND ----------

fb_cols = ["window", "model_id", "slice_key", "slice_value", "predictive_parity", "predictive_equality", "equal_opportunity", "statistical_parity"]
fb_metrics_df = profile_df.select(fb_cols).filter(f"column_name = ':table' AND slice_value = 'true'")
display(fb_metrics_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create data drifts(s) in 3 features
# MAGIC Simulate distribution changes for `workclass`, `gender` and `hours_per_week`

# COMMAND ----------

display(scoring_df2.select(["workclass", "sex", "hours_per_week"]))

# COMMAND ----------

scoring_df2_simulated = (scoring_df2
  # Fill nulls with new class value
  .withColumn("workclass", 
    F.when(F.col("workclass").isNull(), "Digital Nomad")
    .otherwise(F.col("workclass"))
  )
  # Skew gender to Female only
  .withColumn("sex", F.lit("Female"))

  # Skew all hours_per_week with 95
  .withColumn("hours_per_week", F.lit(95).cast("double"))
)
display(scoring_df2_simulated.select(["workclass", "sex", "hours_per_week"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Generate predictions on drifted observations and update inference tables
# MAGIC - Add the column `model_id`

# COMMAND ----------

# Simulate scoring that would happen in 2 days from now
timestamp2 = (datetime.now() + timedelta(2)).timestamp()
pred_df2 = (scoring_df2_simulated
  .withColumn(TIMESTAMP_COL, F.lit(timestamp2).cast("timestamp")) 
  .withColumn(PREDICTION_COL, loaded_model(*features))
  .withColumn(MODEL_ID_COL, F.lit(new_model_version))
  .write.format("delta").mode("append")
  .saveAsTable(TABLE_NAME)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. (Ad-hoc) Join/Update ground-truth labels to inference table
# MAGIC **Note: if ground-truth value can change for a given id through time, then consider also joining/merging on timestamp column**

# COMMAND ----------

# DBTITLE 1,Using MERGE INTO (Recommended)
# Step 1: Create temporary view using new labels
late_labels_view_name = f"adult_census_late_labels_{unique_suffix}"
test_labels_df.createOrReplaceTempView(late_labels_view_name)

# Step 2: Merge into inference table
merge_info = spark.sql(
  f"""
  MERGE INTO {TABLE_NAME} AS i
  USING {late_labels_view_name} AS l
  ON i.{ID_COL} == l.{ID_COL}
  WHEN MATCHED THEN UPDATE SET i.{LABEL_COL} == l.{LABEL_COL}
  """
)
display(merge_info)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. [Optional] Refresh metrics by also adding custom metrics
# MAGIC See the documentation for more details about how to create custom metrics ([AWS](https://docs.databricks.com/lakehouse-monitoring/custom-metrics.html)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/custom-metrics)).

# COMMAND ----------

from pyspark.sql import types as T
from math import exp

CUSTOM_METRICS = [
  lm.Metric(
    type="aggregate",
    name="log_avg",
    input_columns=["fnlwgt"],
    definition="avg(log(abs(`{{input_column}}`)+1))",
    output_data_type=T.DoubleType()
  ),
  lm.Metric(
    type="derived",
    name="exp_log",
    input_columns=["fnlwgt"],
    definition="exp(log_avg)",
    output_data_type=T.DoubleType()
  ),
  lm.Metric(
    type="drift",
    name="delta_exp",
    input_columns=["fnlwgt"],
    definition="{{current_df}}.exp_log - {{base_df}}.exp_log",
    output_data_type=T.DoubleType()
  )
]

# COMMAND ----------

# DBTITLE 1,Update monitor
lm.update_monitor(
  table_name=TABLE_NAME,
  updated_params={"custom_metrics" : CUSTOM_METRICS}
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh metrics and inspect dashboard
# MAGIC
# MAGIC Inspect the auto-generated monitoring DBSQL dashboard ([AWS](https://docs.databricks.com/sql/user/dashboards/index.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/user/dashboards/)).

# COMMAND ----------

run_info = lm.run_refresh(table_name=TABLE_NAME)

while run_info.state in (lm.RefreshState.PENDING, lm.RefreshState.RUNNING):
  run_info = lm.get_refresh(table_name=TABLE_NAME, refresh_id=run_info.refresh_id)
  time.sleep(30)

assert run_info.state == lm.RefreshState.SUCCESS, "Monitor refresh failed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. [Optional] Delete the monitor
# MAGIC Uncomment the following line of code to clean up the monitor (if you wish to run the quickstart on this table again).

# COMMAND ----------

# lm.delete_monitor(table_name=TABLE_NAME)
