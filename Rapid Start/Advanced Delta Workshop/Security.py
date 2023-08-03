# Databricks notebook source
# MAGIC %sql 
# MAGIC show groups

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show users;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use hivedemoyoussef;

# COMMAND ----------

# MAGIC %sql 
# MAGIC --select * from sales;
# MAGIC create view sales_redacted_dashed as 
# MAGIC select  
# MAGIC ORDERNUMBER
# MAGIC QUANTITYORDERED,
# MAGIC PRICEEACH,
# MAGIC ORDERLINENUMBER,
# MAGIC ORDERDATE,
# MAGIC STATUS,
# MAGIC QTR_ID,
# MAGIC MONTH_ID,
# MAGIC YEAR_ID,
# MAGIC PRODUCTLINE, 
# MAGIC case when is_member("sma-demo2") then SALES else '#####' end as SALES from sales;
# MAGIC --case when is_member("sma-demo2") then '####'' else SALES end as SALES from sales; 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use hivedemoyoussef;
# MAGIC select * from sales_redacted_dashed;

# COMMAND ----------

# MAGIC %sql
# MAGIC use hivedemoyoussef;
# MAGIC select * from sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC revoke select on sales from `sma-demo2`;
# MAGIC grant all privileges on sales to `shubham.pachori@databricks.com`;

# COMMAND ----------

# MAGIC %md
# MAGIC Encryption Functions 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select aes_encrypt('hello','abcedfghijklmnop') as encrypted_data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select cast(aes_decrypt(unbase64('2GUAx5kUR8kbUnhHJfbjn62mllPEBj/zNUtoLX9CguXU'),'abcedfghijklmnop') as string) as hidden

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create database if not exists crypto;
# MAGIC use crypto;
# MAGIC drop table if exists crypto.encryption;
# MAGIC create table crypto.encryption
# MAGIC (
# MAGIC Country String,
# MAGIC encryptionKey String
# MAGIC )
# MAGIC using delta;
# MAGIC
# MAGIC
# MAGIC insert into crypto.encryption
# MAGIC select 'Morocco' Country,'abcdjdjfkfkgfdff' encryptionKey
# MAGIC union
# MAGIC select 'France' Country,'abcdjdjfkfkgfdee' encryptionKey
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table if exists crypto.contacts;
# MAGIC
# MAGIC
# MAGIC create table crypto.contacts
# MAGIC (
# MAGIC Name String,
# MAGIC Country String,
# MAGIC Email String
# MAGIC )
# MAGIC using delta;
# MAGIC
# MAGIC insert into crypto.contacts
# MAGIC select 'Youssef' Name,'Morocco' Country, 'youssef.mrini@email.fr' Email
# MAGIC union
# MAGIC select 'Mrini' Name,'France' Country ,'youssef.mrini@databricks.com' Email
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists crypto.coding;
# MAGIC create table crypto.coding
# MAGIC (
# MAGIC Name String,
# MAGIC Country String,
# MAGIC Email String
# MAGIC )
# MAGIC using delta;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   crypto.coding
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       aes_encrypt(Name, encryptionKey) Name,
# MAGIC       c.Country,
# MAGIC       aes_encrypt(Email, encryptionKey) Email
# MAGIC     from
# MAGIC       contacts p
# MAGIC       inner join encryption c on p.Country = c.Country
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace view information as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       aes_encrypt(Name, encryptionKey) Name,
# MAGIC       c.Country,
# MAGIC       aes_encrypt(Email, encryptionKey) Email
# MAGIC     from
# MAGIC       contacts p
# MAGIC       inner join encryption c on p.Country = c.Country
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from information
