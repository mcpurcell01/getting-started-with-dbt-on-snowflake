select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select TRANSACTION_TOTAL_AMOUNT
from INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_TRANSACTIONS
where TRANSACTION_TOTAL_AMOUNT is null



      
    ) dbt_internal_test