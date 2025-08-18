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
FROM {{ source('vending_machine_dbt_db', 'VENDING_TRANSACTIONS') }}
