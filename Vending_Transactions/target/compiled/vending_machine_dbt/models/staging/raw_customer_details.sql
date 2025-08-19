-- This staging model selects all columns from the raw customer details table.
-- File: raw_customer_details.sql
SELECT
    customer_id,
    age,
    gender,
    email_address
FROM INSTRUCTOR1_vending_machine_dbt_db.RAW.CUSTOMER_DETAILS