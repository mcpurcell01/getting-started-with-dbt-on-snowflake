
    
    

select
    TRANSACTION_ID as unique_field,
    count(*) as n_records

from INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_TRANSACTIONS
where TRANSACTION_ID is not null
group by TRANSACTION_ID
having count(*) > 1


