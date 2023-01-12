# Databricks notebook source
# Define the job specification based on specific parameters provided for each api

def generate_job_spec(job_name, job_cluster_key, bronze_task_name, endpoint_param, bronze_notebook, silver_task_name, silver_notebook):
  return {
  "format": "MULTI_TASK",
  "name": job_name,
  "job_clusters": [
    {
      "job_cluster_key": job_cluster_key,
      "new_cluster": {
          "spark_version": "11.3.x-scala2.12",
          "spark_conf": {
              "spark.databricks.delta.preview.enabled": "true"
          },
          "azure_attributes": {
              "first_on_demand": 1,
              "availability": "ON_DEMAND_AZURE",
              "spot_bid_max_price": -1
          },
          "node_type_id": "Standard_DS3_v2",
          "spark_env_vars": {
              "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
          },
          "enable_elastic_disk": True,
          "data_security_mode": "SINGLE_USER",
          "runtime_engine": "STANDARD",
          "num_workers": 2
      }
    }
  ],
  "email_notifications": {
    "no_alert_for_skipped_runs": False,
    "on_failure": [
      "renji.harold@databricks.com"
    ],
    "on_success": [
      "renji.harold@databricks.com"
    ]
  },
  "tags": {
    "cost-center": "marketing-001",
    "team": "marketing"
  },
  "timeout_seconds": 86400,
  "tasks": [
    {
      "task_key": bronze_task_name,
      "description": "Extracts weather data from api",
      "notebook_task": {
          "notebook_path": bronze_notebook,
          "base_parameters": {
              "endpoint": endpoint_param
          },
          "source": "GIT"
      },
      "timeout_seconds": 3600,
      "retry_on_timeout": False,
      "job_cluster_key": "weather_api_cluster"
    },
    {
      "task_key": silver_task_name,
      "description": "Extracts weather data from api",
      "notebook_task": {
          "notebook_path": silver_notebook,          
          "source": "GIT"
      },
      "depends_on": [
        {
          "task_key": bronze_task_name
        }
      ],
      "timeout_seconds": 3600,
      "retry_on_timeout": False,
      "job_cluster_key": "weather_api_cluster"
    }
  ],
  "max_concurrent_runs": 1,
  "schedule": {
    "pause_status": "PAUSED",
    "quartz_cron_expression": "20 30 * * * ?",
    "timezone_id": "Australia/Melbourne"
  },
  "git_source": {
      "git_url": "https://github.com/renjil/db-demo.git",
      "git_provider": "gitHub",
      "git_branch": "main"
  }
}

# COMMAND ----------

# Define the necessary parameters required to trigger the Databricks API

db_admin_username = "token"

# For demo purposes I have added the token and URL as a variable. In Production the recommendation is to use a key store/vault like Secrets
# For e.g. dbutils.secrets.get('mysecrets', 'secret_token')
db_admin_token="XXXX"
db_url = "adb-XXXX.XX.azuredatabricks.net"

# COMMAND ----------

import requests
from requests.auth import HTTPBasicAuth
import json

# COMMAND ----------

# function that triggers the job api

def create_job(job_spec):
  response = requests.post(f"https://{db_url}/api/2.1/jobs/create",
                        headers = {'Content-type': 'application/json'},
                        auth=HTTPBasicAuth(db_admin_username, db_admin_token),
                        data=json.dumps(job_spec))
  response.raise_for_status()
  return response.json().get("job_id")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Successfully imported functions:
# MAGIC - generate_job_spec
# MAGIC - create_job

# COMMAND ----------


