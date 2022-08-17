-- Databricks notebook source
create catalog if not exists renji_demo

-- COMMAND ----------

use catalog renji_demo

-- COMMAND ----------

create schema if not exists reece_demo

-- COMMAND ----------

use schema reece_demo

-- COMMAND ----------

create table sales_agg_gold
(
  id int,
  branch_code int,
  amount float,
  day int,
  month int,
  year int
)

-- COMMAND ----------

describe extended sales_agg_gold

-- COMMAND ----------

insert into sales_agg_gold values 
(7, 1200, 01, 09, 2022, 'BR001'),
(8,  200, 01, 10, 2022, 'BR001'),
(9, 1100, 01, 11, 2022, 'BR002'),
(10, 1000, 01, 12, 2022, 'BR002'),
(1, 500, 01, 09, 2019, 'BR001'),
(2, 700, 01, 08, 2019, 'BR002'),
(5, 1000, 01, 03, 2021, 'BR001'),
(6, 1100, 01, 04, 2021, 'BR002'),
(3, 300, 01, 06, 2020, 'BR001'),
(4, 800, 01, 07, 2020, 'BR002');

-- COMMAND ----------

select * from sales_agg_gold ;

-- COMMAND ----------

delete from sales_agg_gold;

-- COMMAND ----------

ALTER TABLE sales_agg_gold SET TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')

-- COMMAND ----------

create schema hive_metatore.renjidemo

-- COMMAND ----------

use catalog hive_metastore

-- COMMAND ----------

create schema hive_metastore.renjidemo;

-- COMMAND ----------

use schema renjidemo;

-- COMMAND ----------

create table sales_agg_gold
as
select * from renji_demo.reece_demo.sales_agg_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC solution - same cube like functionality
-- MAGIC cube - pull metric
-- MAGIC 
-- MAGIC connect to cube via excel
-- MAGIC 
-- MAGIC daily sales metric
-- MAGIC 
-- MAGIC total daily sales 
-- MAGIC 
-- MAGIC daily sales per branch - drag and drop branch code
-- MAGIC 
-- MAGIC sales for this year - drag and drop calendar dimension - select year and month - sales for june 2020 for all branches
-- MAGIC 
-- MAGIC pain points:
-- MAGIC - cubes with overlapping domains
-- MAGIC - metrics not updated across cubes
-- MAGIC - sales cube is a mammoth complex cube - not confident to make changes to cube
-- MAGIC - visual studio for managing changes, and foundationdb
-- MAGIC - 
