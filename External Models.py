# Databricks notebook source
# MAGIC %pip install mlflow[genai]>=2.9.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")
client.create_endpoint(
    name="openaiyoussef",
    config={
        "served_entities": [{
            "external_model": {
                "name": "gpt-3.5-turbo",
                "provider": "openai",
                "task": "llm/v1/chat",
                "openai_config": {
                    "openai_api_key": "{{secrets/openaiyoussef/openai}}"
                }
            }
        }],
        "rate_limits": [
            {
                "key": "user",
                "renewal_period": "minute",
                "calls": 10
            }
        ]
    }
)
