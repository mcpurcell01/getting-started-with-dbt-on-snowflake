
    
    

select
    CUSTOMER_ID as unique_field,
    count(*) as n_records

from INSTRUCTOR1_vending_machine_dbt_db.RAW.CUSTOMER_DETAILS
where CUSTOMER_ID is not null
group by CUSTOMER_ID
having count(*) > 1


