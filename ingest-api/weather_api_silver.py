# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Curate and transform weather data to make it more readable

# COMMAND ----------

from pyspark.sql.functions import current_date, explode

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from weather_api_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Here we will explode the array and split each cell into separate columns

# COMMAND ----------

df = spark.sql("select * from weather_api_bronze")
display(df)

# COMMAND ----------

df_explode = df.withColumn("data", explode(df.dataseries)).drop(df.dataseries)
display(df_explode)

# COMMAND ----------

df_split = df_explode.select("init", "product", "created_date", "data.*")
display(df_split)

# COMMAND ----------

# MAGIC %md Save dataframe to a Delta table in Databricks

# COMMAND ----------

df_split.write.mode('append').saveAsTable("weather_api_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC Query data from table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM weather_api_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Data can now be used for further transformations and analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- find average temperature for the day 
# MAGIC 
# MAGIC SELECT AVG(temp2m) as avg_temp from weather_api_silver

# COMMAND ----------


