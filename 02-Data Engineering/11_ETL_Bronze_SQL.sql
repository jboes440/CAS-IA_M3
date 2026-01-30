-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Ingestion in the Bronze layer
-- MAGIC
-- MAGIC ## Connecting to the bronze layer (Target)

-- COMMAND ----------

USE CATALOG jul_lakehouse;
USE DATABASE bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load data into bronze layer of the Lakehouse

-- COMMAND ----------

CREATE OR REPLACE TABLE address 
AS SELECT * FROM jul_adventureworks.saleslt.address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderDetail

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderDetail
AS SELECT * FROM jul_adventureworks.saleslt.SalesOrderDetail;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of SalesOrderHeader

-- COMMAND ----------

CREATE OR REPLACE TABLE SalesOrderHeader
AS SELECT * FROM jul_adventureworks.saleslt.SalesOrderHeader;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Product

-- COMMAND ----------

CREATE OR REPLACE TABLE Product
AS SELECT * FROM jul_adventureworks.saleslt.Product;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductCategory

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductCategory
AS SELECT * FROM jul_adventureworks.saleslt.ProductCategory;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Address
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE Address 
AS SELECT * FROM jul_adventureworks.saleslt.Address;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Customer

-- COMMAND ----------

CREATE OR REPLACE TABLE Customer 
AS SELECT * FROM jul_adventureworks.saleslt.Customer;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductModel

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModel 
AS SELECT * FROM jul_adventureworks.saleslt.productmodel;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductModelProductDescription

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductModelProductDescription
AS SELECT * FROM jul_adventureworks.saleslt.productmodelproductdescription;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of ProductDescription

-- COMMAND ----------

CREATE OR REPLACE TABLE ProductDescription
AS SELECT * FROM jul_adventureworks.saleslt.productdescription;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ingestion of Customer Address

-- COMMAND ----------

CREATE OR REPLACE TABLE CustomerAddress
AS SELECT * FROM jul_adventureworks.saleslt.customeraddress;
