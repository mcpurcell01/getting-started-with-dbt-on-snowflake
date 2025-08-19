with vending_transactions as (

select
    vt.transaction_id,
    vt.transaction_timestamp,
    vt.product_id,
    vt.quantity,
    vt.transaction_total_amount,
    pd.product_name,
    pd.product_category
from INSTRUCTOR2_vending_machine_dbt_db.dev.raw_vending_transactions vt
inner join INSTRUCTOR2_vending_machine_dbt_db.dev.raw_product_details pd on vt.product_id = pd.product_id
)

select

product_name,
product_category,
date_trunc('day', transaction_timestamp) as sales_date,
sum(quantity) as total_items_sold,
sum(transaction_total_amount) as total_revenue,
count(distinct transaction_id) as total_transactions
from vending_transactions group by 1, 2, 3 order by 1, 2, 3