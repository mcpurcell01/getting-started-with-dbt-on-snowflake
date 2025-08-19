select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    TRANSACTION_ID as unique_field,
    count(*) as n_records

from INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_TRANSACTIONS
where TRANSACTION_ID is not null
group by TRANSACTION_ID
having count(*) > 1



      
    ) dbt_internal_test