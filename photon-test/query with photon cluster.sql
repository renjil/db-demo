-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Examining queries run on a cluster with photon enabled

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS PHOTON_AMA_TEST; 
USE PHOTON_AMA_TEST;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS nyctaxi_yellowcab_table_photon
OPTIONS (
  path "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow"
);

-- COMMAND ----------

-- DBTITLE 1,There are about ~1.6B rows of data
SELECT COUNT(*) FROM nyctaxi_yellowcab_table_photon;

-- COMMAND ----------

-- DBTITLE 1,Benchmark: we run a simple aggregation query over this data
SELECT
  vendor_id,
  SUM(trip_distance) AS suim_trip_distance,
  AVG(trip_distance) AS avg_trip_distance
FROM
  nyctaxi_yellowcab_table_photon
WHERE
  passenger_count IN (1, 2, 4)
GROUP BY
  vendor_id
ORDER BY
  vendor_id;

-- COMMAND ----------

-- DBTITLE 1,List expressions supported by Photon
-- MAGIC %scala
-- MAGIC 
-- MAGIC import com.databricks.photon.PhotonSupport
-- MAGIC 
-- MAGIC display(PhotonSupport.supportedExpressions(spark))
