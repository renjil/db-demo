# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1 - Setup API invocation methods

# COMMAND ----------

# MAGIC %md
# MAGIC Create an input widget to specify the API endpoint url

# COMMAND ----------

dbutils.widgets.text("endpoint", "https://api2.binance.com/api/v3/ticker/24hr")

# COMMAND ----------

# MAGIC %md
# MAGIC Display the endpoint captured in the widget

# COMMAND ----------

display(getArgument("endpoint"))

# COMMAND ----------

# MAGIC %md
# MAGIC Define the function that will trigger the endpoint and receive the response. 
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
# MAGIC Define the function that writes the response from API to a local file. 
# MAGIC 
# MAGIC In this example the response is in JSON format. The function can be enhanced to handle multiple formats

# COMMAND ----------

import json
import time

def write_data(data):
  current_time = time.strftime("%Y%m%d-%H%M%S")
  filename = "weatherdata_"+current_time+".json"
  path = "/dbfs/FileStore/renji.harold/api/weather/"
  output_file = path+filename
  print("output file: "+output_file)
  try:
    with open(output_file, 'w') as f:
        json.dump(data, f)
    return output_file
  except Exception as err:
    print("error while writing output")
    print(err)

# COMMAND ----------

# MAGIC %md Note - Defining the above two functions can be moved into its own notebook and imported into the main notebook to keep it cleaner. Leaving it here for easy reading.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Ingest the data from API locally

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Invoke both functions to call the API and write the data locally to a JSON file

# COMMAND ----------

data = request_data(getArgument("endpoint"))
output_file = write_data(data.json())


# COMMAND ----------

# MAGIC %md
# MAGIC Check what the response data looks like

# COMMAND ----------

data.json()

# COMMAND ----------

# MAGIC %md
# MAGIC List local file system to see the files that have been written

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/renji.harold/api/weather/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Curate and transform the data as required and write to a delta table

# COMMAND ----------

# MAGIC %md
# MAGIC Read the file using spark and save into a dataframe for analysis

# COMMAND ----------

from pyspark.sql.functions import explode, flatten, from_json, posexplode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# COMMAND ----------

df = spark.read.json(output_file[5:])
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Here we will explode the array and split each cell into separate columns

# COMMAND ----------

df_explode = df.withColumn("data", explode(df.dataseries)).drop(df.dataseries)
display(df_explode)

# COMMAND ----------

# MAGIC %md
# MAGIC Note - Below code from here on out is specific to the weather API, so would ideally sit outside of this notebook and in a separate pipeline intended for further processing of the specific data set.
# MAGIC That would allow for the API Ingestion notebook to be generic

# COMMAND ----------

df_split = df_explode.select("init", "product", "data.*")
display(df_splitdf_explode.select("init", "product", "data.*"))

# COMMAND ----------

# MAGIC %md Save dataframe to a Delta table in Databricks

# COMMAND ----------

df_split.write.mode('append').saveAsTable("weather_api_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC Query data from table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM weather_api_demo

# COMMAND ----------

# MAGIC %md
# MAGIC Data can now be used for further transformations and analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- find average temperature for the day 
# MAGIC 
# MAGIC SELECT AVG(temp2m) as avg_temp from weather_api_demo

# COMMAND ----------


