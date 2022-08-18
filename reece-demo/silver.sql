-- Databricks notebook source
create catalog if not exists renji_demo;
use catalog renji_demo;
create database if not exists reece_demo;
use database reece_demo

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
where month = 9 and year = 2022;

-- COMMAND ----------

select 
  id,
  sum(amount) over (partition by month, year, branch_code) as monthly_sales,
  month,
  year,
  branch_code
from sales_details_silver
order by id;

