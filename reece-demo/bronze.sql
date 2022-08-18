-- Databricks notebook source
-- set context
%run ./Utils/Init

-- COMMAND ----------

create table if not exists branch_details_bronze
(
branch_id string,
branch_name string,
state string
)

-- COMMAND ----------

insert into branch_details_bronze values
('BR001', 'VIC Downtown branch', 'VIC'),
('BR002', 'VIC Suburban branch', 'VIC'),
('BR003', 'NSW Downtown branch', 'NSW'),
('BR004', 'NSW Suburban branch', 'NSW'),
('BR005', 'QLD Downtown branch', 'QLD'),
('BR006', 'QLD Suburban branch', 'QLD');

-- COMMAND ----------

create table if not exists sales_details_bronze
(
id int,
amount float,
day int,
month int,
year int,
branch_code string,
product_code string,
sales_rep string
)

-- COMMAND ----------

insert into sales_details_bronze values 
(1, 1200, 01, 09, 2022, 'BR001', 'PR001', 'Tom Hanks'),
(2,  200, 01, 10, 2022, 'BR001', 'PR001', 'John Doe'),
(3, 1100, 01, 11, 2022, 'BR002', 'PR001', 'Jim Bills'),
(4, 1000, 01, 12, 2022, 'BR002', 'PR001', 'Rose Hill'),
(5, 500, 01, 09, 2019, 'BR001', 'PR001', 'Tom Hanks'),
(6, 700, 01, 08, 2019, 'BR002', 'PR001', 'Kate Banks'),
(7, 1000, 01, 03, 2021, 'BR001', 'PR001', 'Robert Knox'),
(8, 1100, 01, 04, 2021, 'BR002', 'PR001', 'Will Burt'),
(9, 300, 01, 06, 2020, 'BR001', 'PR001', 'Bill Barr'),
(10, 800, 01, 07, 2020, 'BR002', 'PR001', 'Tom Hanks'),
(11, 200, 01, 09, 2022, 'BR003', 'PR001', 'Tom Hanks'),
(12, 300, 01, 10, 2022, 'BR003', 'PR001', 'John Doe'),
(13, 100, 01, 11, 2022, 'BR004', 'PR001', 'Jim Bills'),
(14, 600, 01, 12, 2022, 'BR004', 'PR001', 'Rose Hill'),
(15, 50, 01, 09, 2019, 'BR005', 'PR001', 'Tom Hanks'),
(16, 400, 01, 08, 2019, 'BR006', 'PR001', 'Kate Banks'),
(17, 300, 01, 03, 2021, 'BR001', 'PR001', 'Robert Knox'),
(18, 150, 01, 04, 2021, 'BR002', 'PR001', 'Will Burt'),
(19, 350, 01, 06, 2020, 'BR003', 'PR001', 'Bill Barr'),
(20, 930, 01, 07, 2020, 'BR004', 'PR001', 'Tom Hanks');

-- COMMAND ----------

select * from branch_details_bronze

-- COMMAND ----------

select * from sales_details_bronze order by id;
