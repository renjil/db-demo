-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Examining queries run on a cluster with photon disabled

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS PHOTON_AMA_TEST; 
USE PHOTON_AMA_TEST;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS nyctaxi_yellowcab_table_nophoton
OPTIONS (
  path "/databricks-datasets/nyctaxi/tables/nyctaxi_yellow"
);

-- COMMAND ----------

-- DBTITLE 1,There are about ~1.6B rows of data
SELECT COUNT(*) FROM nyctaxi_yellowcab_table_nophoton;

-- COMMAND ----------

-- DBTITLE 1,Benchmark: we run a simple aggregation query over this data
SELECT
  vendor_id,
  SUM(trip_distance) AS suim_trip_distance,
  AVG(trip_distance) AS avg_trip_distance
FROM
  nyctaxi_yellowcab_table_nophoton
WHERE
  passenger_count IN (1, 2, 4)
GROUP BY
  vendor_id
ORDER BY
  vendor_id;

-- COMMAND ----------

-- drop table PHOTON_AMA_TEST.nyctaxi_yellowcab_table_nophoton
