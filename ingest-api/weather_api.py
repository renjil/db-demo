# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC ## Collect weather data from Weather API

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC <img style="float:right" src="https://github.com/renjil/db-demo/raw/main/ingest-api/ingest_api.png" width="300" height="300"/>
# MAGIC 
# MAGIC Clone this notebook to create jobs for different API's
# MAGIC 
# MAGIC Here for the weather API, we define parameters specific for the job.
# MAGIC 
# MAGIC The job creates two tasks:
# MAGIC - first tasks ingests data from API into a bronze table
# MAGIC - second task ingests data from bronze to silver
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Note - You can further parameterize it based on your requirements e.g. cluster size, schedule etc.
# MAGIC 
# MAGIC Refer here for full job spec -> https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate

# COMMAND ----------

job_name = "Ingest weather API"
job_cluster_key = "weather_api_cluster"
bronze_task_name = "api_to_bronze"
endpoint_param = "http://www.7timer.info/bin/api.pl?lon=113.17&lat=23.09&product=astro&output=json"
bronze_notebook = "ingest-api/api_to_bronze"
silver_task_name = "bronze_to_silver"
silver_notebook = "ingest-api/weather_api_silver"

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Import the job_api_utils notebook to make available necessary helper functions

# COMMAND ----------

# MAGIC %run ./utils/job_api_utils

# COMMAND ----------

job_spec = generate_job_spec(job_name, job_cluster_key, bronze_task_name, endpoint_param, bronze_notebook, silver_task_name, silver_notebook)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Job Spec is a JSON definition of the Databricks Job. 
# MAGIC 
# MAGIC We have configured the spec to contain two tasks

# COMMAND ----------

json.dumps(job_spec)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Create a Databricks Job

# COMMAND ----------

job_id = create_job(job_spec)

# COMMAND ----------

f"Databricks JOB id is {job_id}"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Go to the Databricks Workflows UI and view the job there. 
# MAGIC 
# MAGIC Have a look at the various configurations - schedule, notifications, compute etc.
# MAGIC 
# MAGIC Trigger the job to see how it behaves.

# COMMAND ----------


