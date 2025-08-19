
  create or replace   view INSTRUCTOR2_vending_machine_dbt_db.dev.raw_vending_transactions
  
   as (
    -- This staging model selects all columns from the raw vending transactions table.
-- It's a foundational model that other models will build upon.
SELECT
    transaction_id,
    vending_machine_id,
    transaction_timestamp,
    product_id,
    quantity,
    transaction_total_amount,
    payment_method,
    customer_id
FROM INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_TRANSACTIONS
  );

