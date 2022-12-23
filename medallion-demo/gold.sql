-- Databricks notebook source
-- set context
%run ./Utils/Init

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a table that aggregates data to give a view for monthly sales details for each branch

-- COMMAND ----------

create table if not exists sales_monthly_agg_gold
as
select 
  id,
  sum(amount) over (partition by month, year, branch_code) as monthly_sales,
  month,
  year,
  branch_code
from sales_details_silver
order by id;

-- COMMAND ----------

select * from sales_monthly_agg_gold;

-- COMMAND ----------



-- COMMAND ----------

-- move table to hive_metastore
create table if not exists hive_metastore.renjidemo.sales_monthly_agg_gold
as
select * from renji_demo.reece_demo.sales_monthly_agg_gold;

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
