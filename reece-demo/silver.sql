-- Databricks notebook source
-- set context
%run ./Utils/Init

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Refine silver table with only required attributes

-- COMMAND ----------

-- test again
create table if not exists sales_details_silver
as
select 
  id,
  amount,
  day,
  month,
  year,
  branch_code
from sales_details_bronze
where amount is not null
and branch_code is like 'BR%';

-- COMMAND ----------

select * from sales_details_silver
order by id;
