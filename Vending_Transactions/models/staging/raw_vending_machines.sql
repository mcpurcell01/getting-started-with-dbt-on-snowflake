-- This staging model selects all columns from the raw vending machines table.
SELECT
    vending_machine_id,
    location_city,
    region,
    country
FROM {{ source('vending_machine_dbt_db', 'VENDING_MACHINES') }}