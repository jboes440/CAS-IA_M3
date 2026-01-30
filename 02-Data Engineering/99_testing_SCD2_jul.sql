-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Code to test the SCD2 in Silver

-- COMMAND ----------

USE CATALOG jul_lakehouse;
USE SCHEMA bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Simulate an UPDATE in source

-- COMMAND ----------

SELECT * FROM salesorderdetail WHERE ProductID = 836;

-- COMMAND ----------

SELECT * FROM salesorderdetail where ProductID = 810;

-- COMMAND ----------

UPDATE salesorderdetail SET UnitPrice = 365.0, ModifiedDate = current_timestamp() WHERE ProductID = 836;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Simulate a DELETE in source

-- COMMAND ----------

SELECT * FROM salesorderdetail WHERE ProductID = 810;

-- COMMAND ----------

DELETE FROM salesorderdetail WHERE ProductID = 810;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Simulate an INSERT in source

-- COMMAND ----------

SELECT * FROM bronze.salesorderdetail ORDER BY ProductID DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Due to laziness we shall cheat and just modify the primary key, i.e. it's actually an INSERT and a DELETE.

-- COMMAND ----------

UPDATE bronze.salesorderdetail SET ProductID = 1000 WHERE ProductID = 999;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## !!! Run the ETL and come back here to test

-- COMMAND ----------

USE CATALOG jul_lakehouse;
USE SCHEMA silver;

SELECT * FROM sales_order_detail WHERE Product_ID = 810 ORDER BY sales_order_id, _tf_valid_from

-- COMMAND ----------

SELECT * FROM sales_order_detail WHERE Product_ID = 836 ORDER BY sales_order_id, _tf_valid_from

-- COMMAND ----------

SELECT * FROM sales_order_detail WHERE product_id IN (999, 1000);
