select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select CUSTOMER_ID
from INSTRUCTOR1_vending_machine_dbt_db.RAW.CUSTOMER_DETAILS
where CUSTOMER_ID is null



      
    ) dbt_internal_test