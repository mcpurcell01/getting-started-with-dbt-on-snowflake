select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select TRANSACTION_ID
from INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_TRANSACTIONS
where TRANSACTION_ID is null



      
    ) dbt_internal_test