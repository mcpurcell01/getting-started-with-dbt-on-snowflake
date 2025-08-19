-- This staging model selects all columns from the raw product details table.
-- File: raw_product_details.sql
SELECT
    product_id,
    product_name,
    product_category,
    unit_price
FROM {{ source('vending_machine_dbt_db', 'PRODUCT_DETAILS') }}
