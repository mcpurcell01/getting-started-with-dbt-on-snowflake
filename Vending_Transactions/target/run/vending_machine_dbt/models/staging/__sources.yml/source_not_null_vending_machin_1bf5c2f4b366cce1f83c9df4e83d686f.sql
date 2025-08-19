select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select PRODUCT_NAME
from INSTRUCTOR1_vending_machine_dbt_db.RAW.PRODUCT_DETAILS
where PRODUCT_NAME is null



      
    ) dbt_internal_test