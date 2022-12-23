# Databricks notebook source
dbutils.fs.put("/tmp/test.json", """
{"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
{"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
{"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}
""", True)

# COMMAND ----------

dbutils.fs.ls("/tmp/test.json")

# COMMAND ----------

testJsonData = spark.read.json("/tmp/test.json")

display(testJsonData)

# COMMAND ----------

testJsonData.write.mode("overwrite").format("delta").saveAsTable("renji_demo_json")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from renji_demo_json

# COMMAND ----------

# MAGIC %sql
# MAGIC select array[0] from renji_demo_json

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW jsonTable
# MAGIC USING json
# MAGIC OPTIONS (path="/tmp/test.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jsonTable

# COMMAND ----------


