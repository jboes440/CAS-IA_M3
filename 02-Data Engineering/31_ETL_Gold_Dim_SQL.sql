-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Loading the Dim tables in the Gold layer 
-- MAGIC ## Connecting to the Gold layer (Target)

-- COMMAND ----------

USE CATALOG jul_lakehouse;
USE SCHEMA gold;

DECLARE OR REPLACE load_date = current_timestamp();
VALUES load_date;

-- COMMAND ----------

MERGE INTO gold.dim_geography AS tgt
USING (
    SELECT
        CAST(address_id AS INT) AS geo_address_id,
        COALESCE(TRY_CAST(address_line1 AS STRING), 'N/A') AS geo_address_line_1,
        COALESCE(TRY_CAST(address_line2 AS STRING), 'N/A') AS geo_address_line_2,
        COALESCE(TRY_CAST(city AS STRING), 'N/A') AS geo_city,
        COALESCE(TRY_CAST(state_province AS STRING), 'N/A') AS geo_state_province,
        COALESCE(TRY_CAST(country_region AS STRING), 'N/A') AS geo_country_region,
        COALESCE(TRY_CAST(postal_code AS STRING), 'N/A') AS geo_postal_code
    FROM silver.address
    WHERE _tf_valid_to IS NULL
) AS src
ON tgt.geo_address_id = src.geo_address_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.geo_address_line_1 != src.geo_address_line_1 OR 
    tgt.geo_address_line_2 != src.geo_address_line_2 OR 
    tgt.geo_city != src.geo_city OR
    tgt.geo_state_province != src.geo_state_province OR
    tgt.geo_country_region != src.geo_country_region OR
    tgt.geo_postal_code != src.geo_postal_code
) THEN 
  
  UPDATE SET 
    tgt.geo_address_line_1 = src.geo_address_line_1,
    tgt.geo_address_line_2 = src.geo_address_line_2,
    tgt.geo_city = src.geo_city,
    tgt.geo_state_province = src.geo_state_province,
    tgt.geo_country_region = src.geo_country_region,
    tgt.geo_postal_code = src.geo_postal_code,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  
  INSERT (
    geo_address_id,
    geo_address_line_1,
    geo_address_line_2,
    geo_city,
    geo_state_province,
    geo_country_region,
    geo_postal_code,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.geo_address_id,
    src.geo_address_line_1,
    src.geo_address_line_2,
    src.geo_city,
    src.geo_state_province,
    src.geo_country_region,
    src.geo_postal_code,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

MERGE INTO gold.dim_customer AS tgt
USING (
    SELECT
        CAST(customer_id AS INT) AS cust_customer_id,
        COALESCE(TRY_CAST(title AS STRING), 'N/A') AS cust_title,
        COALESCE(TRY_CAST(first_name AS STRING), 'N/A') AS cust_first_name,
        COALESCE(TRY_CAST(middle_name AS STRING), 'N/A') AS cust_middle_name,
        COALESCE(TRY_CAST(last_name AS STRING), 'N/A') AS cust_last_name,
        COALESCE(TRY_CAST(suffix AS STRING), 'N/A') AS cust_suffix,
        COALESCE(TRY_CAST(company_name AS STRING), 'N/A') AS cust_company_name,
        COALESCE(TRY_CAST(sales_person AS STRING), 'N/A') AS cust_sales_person,
        COALESCE(TRY_CAST(email_address AS STRING), 'N/A') AS cust_email_address,
        COALESCE(TRY_CAST(phone AS STRING), 'N/A') AS cust_phone
    FROM silver.customer
    WHERE _tf_valid_to IS NULL
) AS src
ON tgt.cust_customer_id = src.cust_customer_id

-- 1) Update existing records when a difference is detected
WHEN MATCHED AND (
    tgt.cust_title != src.cust_title OR
    tgt.cust_first_name != src.cust_first_name OR
    tgt.cust_middle_name != src.cust_middle_name OR
    tgt.cust_last_name != src.cust_last_name OR
    tgt.cust_suffix != src.cust_suffix OR
    tgt.cust_company_name != src.cust_company_name OR
    tgt.cust_sales_person != src.cust_sales_person OR
    tgt.cust_email_address != src.cust_email_address OR
    tgt.cust_phone != src.cust_phone
) THEN 
  
  UPDATE SET 
    tgt.cust_title = src.cust_title,
    tgt.cust_first_name = src.cust_first_name,
    tgt.cust_middle_name = src.cust_middle_name,
    tgt.cust_last_name = src.cust_last_name,
    tgt.cust_suffix = src.cust_suffix,
    tgt.cust_company_name = src.cust_company_name,
    tgt.cust_sales_person = src.cust_sales_person,
    tgt.cust_email_address = src.cust_email_address,
    tgt.cust_phone = src.cust_phone,
    tgt._tf_update_date = load_date

-- 2) Insert new records
WHEN NOT MATCHED THEN
  
  INSERT (
    cust_customer_id,
    cust_title,
    cust_first_name,
    cust_middle_name,
    cust_last_name,
    cust_suffix,
    cust_company_name,
    cust_sales_person,
    cust_email_address,
    cust_phone,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.cust_customer_id,
    src.cust_title,
    src.cust_first_name,
    src.cust_middle_name,
    src.cust_last_name,
    src.cust_suffix,
    src.cust_company_name,
    src.cust_sales_person,
    src.cust_email_address,
    src.cust_phone,
    load_date,        -- _tf_create_date
    load_date         -- _tf_update_date
  )

-- COMMAND ----------

MERGE INTO gold.dim_product AS tgt
USING (
    SELECT
        CAST(product_id AS INT) AS prod_product_id,
        COALESCE(TRY_CAST(name AS STRING), 'N/A') AS prod_name,
        COALESCE(TRY_CAST(number AS STRING), 'N/A') AS prod_number,
        COALESCE(TRY_CAST(color AS STRING), 'N/A') AS prod_color,
        COALESCE(TRY_CAST(cost AS DECIMAL(38,6)), 0) AS prod_cost,
        COALESCE(TRY_CAST(price AS DECIMAL(38,6)), 0) AS prod_price,
        COALESCE(TRY_CAST(size AS STRING), 'N/A') AS prod_size,
        COALESCE(TRY_CAST(weight AS DECIMAL(19,4)), 0) AS prod_weight,
        COALESCE(TRY_CAST(product_category_id AS INT), -9) AS prod_product_category_id
    FROM silver.product
    WHERE _tf_valid_to IS NULL
) AS src
ON tgt.prod_product_id = src.prod_product_id

WHEN MATCHED AND (
    tgt.prod_name != src.prod_name OR 
    tgt.prod_number != src.prod_number OR 
    tgt.prod_color != src.prod_color OR
    tgt.prod_cost != src.prod_cost OR
    tgt.prod_price != src.prod_price OR
    tgt.prod_size != src.prod_size OR
    tgt.prod_weight != src.prod_weight OR
    tgt.prod_product_category_id != src.prod_product_category_id
) THEN 
  
  UPDATE SET 
    tgt.prod_name = src.prod_name,
    tgt.prod_number = src.prod_number,
    tgt.prod_color = src.prod_color,
    tgt.prod_cost = src.prod_cost,
    tgt.prod_price = src.prod_price,
    tgt.prod_size = src.prod_size,
    tgt.prod_weight = src.prod_weight,
    tgt.prod_product_category_id = src.prod_product_category_id,
    tgt._tf_update_date = load_date

WHEN NOT MATCHED THEN
  
  INSERT (
    prod_product_id,
    prod_name,
    prod_number,
    prod_color,
    prod_cost,
    prod_price,
    prod_size,
    prod_weight,
    prod_product_category_id,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.prod_product_id,
    src.prod_name,
    src.prod_number,
    src.prod_color,
    src.prod_cost,
    src.prod_price,
    src.prod_size,
    src.prod_weight,
    src.prod_product_category_id,
    load_date,
    load_date
  )

-- COMMAND ----------

MERGE INTO gold.dim_product_category AS tgt
USING (
    SELECT
        CAST(product_category_id AS INT) AS cat_product_category_id,
        COALESCE(TRY_CAST(name AS STRING), 'N/A') AS cat_name
    FROM silver.product_category
    WHERE _tf_valid_to IS NULL
) AS src
ON tgt.cat_product_category_id = src.cat_product_category_id

WHEN MATCHED AND (
    tgt.cat_name != src.cat_name
) THEN 
  
  UPDATE SET 
    tgt.cat_name = src.cat_name,
    tgt._tf_update_date = load_date

WHEN NOT MATCHED THEN
  
  INSERT (
    cat_product_category_id,
    cat_name,
    _tf_create_date,
    _tf_update_date
  )
  VALUES (
    src.cat_product_category_id,
    src.cat_name,
    load_date,
    load_date
  )
