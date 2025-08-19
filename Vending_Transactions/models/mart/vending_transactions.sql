-- This model joins various raw tables related to vending transactions to create a single, comprehensive view.
-- It links transactions to customer details, products, and vending machine information.
SELECT
    vt.transaction_id,
    vt.transaction_timestamp,
    vt.vending_machine_id,
    vt.product_id,
    pd.product_name,
    pd.product_category,
    pd.unit_price,
    vt.quantity,
    vt.transaction_total_amount,
    vt.payment_method,
    vm.location_city,
    vm.region,
    vm.country,
    c.customer_id,
    c.age,
    c.gender,
    c.email_address
FROM {{ ref('raw_vending_transactions') }} vt
JOIN {{ ref('raw_product_details') }} pd
    ON vt.product_id = pd.product_id
JOIN {{ ref('raw_vending_machines') }} vm
    ON vt.vending_machine_id = vm.vending_machine_id
LEFT JOIN {{ ref('raw_customer_details') }} c
    ON vt.customer_id = c.customer_id