
  create or replace   view INSTRUCTOR2_vending_machine_dbt_db.dev.raw_product_details
  
   as (
    -- This staging model selects all columns from the raw product details table.
-- File: raw_product_details.sql
SELECT
    product_id,
    product_name,
    product_category,
    unit_price
FROM INSTRUCTOR1_vending_machine_dbt_db.RAW.PRODUCT_DETAILS
  );

