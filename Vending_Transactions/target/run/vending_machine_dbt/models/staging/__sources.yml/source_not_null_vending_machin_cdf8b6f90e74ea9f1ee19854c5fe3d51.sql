select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select QUANTITY
from INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_TRANSACTIONS
where QUANTITY is null



      
    ) dbt_internal_test