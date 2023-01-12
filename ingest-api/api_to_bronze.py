# Databricks notebook source
# MAGIC %md 
# MAGIC 
# MAGIC # Generic notebook that can ingest data from an api and save it to a delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Parameterize notebook to receive endpoint url

# COMMAND ----------

# MAGIC %md
# MAGIC Create an input widget to specify the API endpoint url

# COMMAND ----------

dbutils.widgets.text("endpoint", "http://www.7timer.info/bin/api.pl?lon=113.17&lat=23.09&product=astro&output=json")

# COMMAND ----------

# MAGIC %md
# MAGIC Display the endpoint captured in the widget

# COMMAND ----------

display(getArgument("endpoint"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Ingest the data from API

# COMMAND ----------

# MAGIC %md 
# MAGIC Define function that can invoke an API and return response
# MAGIC 
# MAGIC This function can be enhanced to be more sophisticated where necessary. 
# MAGIC 
# MAGIC For e.g - pass parameters, add auth, add pagination etc.

# COMMAND ----------

import requests

def request_data(endpoint):
  try:
    response = requests.get(url=endpoint)
    response.raise_for_status()
    return response
  except requests.exceptions.HTTPError as errh:
    print(errh)
  except requests.exceptions.ConnectionError as errc:
      print(errc)
  except requests.exceptions.Timeout as errt:
      print(errt)
  except requests.exceptions.RequestException as err:
      print(err)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Invoke function to call the API 

# COMMAND ----------

data = request_data(getArgument("endpoint"))


# COMMAND ----------

# MAGIC %md
# MAGIC Check what the response data looks like

# COMMAND ----------

data.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Save data to a delta table

# COMMAND ----------

# MAGIC %md
# MAGIC Read the file using spark and save into a dataframe for analysis

# COMMAND ----------

from pyspark.sql.functions import current_date

# COMMAND ----------

# df = spark.read.json(output_file[5:])
df = spark.read.json(sc.parallelize([ data.text ]))
df = df.withColumn("created_date", current_date())
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Write the data to a bronze table

# COMMAND ----------

df.write.mode('append').saveAsTable("weather_api_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Query data from table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from weather_api_bronze

# COMMAND ----------


