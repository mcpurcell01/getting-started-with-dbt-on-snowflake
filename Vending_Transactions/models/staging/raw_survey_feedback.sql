-- This staging model selects all columns from the raw survey feedback table.
SELECT
    survey_id,
    transaction_id,
    customer_id,
    satisfaction_score
FROM {{ source('vending_dbt_db', 'SURVEY_FEEDBACK') }}