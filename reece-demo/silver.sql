-- Databricks notebook source
-- set context
%run ./Utils/Init

-- COMMAND ----------

create table if not exists sales_details_silver
as
select 
  id,
  amount,
  day,
  month,
  year,
  branch_code
from sales_details_bronze;

-- COMMAND ----------

select * from sales_details_silver
order by id;
