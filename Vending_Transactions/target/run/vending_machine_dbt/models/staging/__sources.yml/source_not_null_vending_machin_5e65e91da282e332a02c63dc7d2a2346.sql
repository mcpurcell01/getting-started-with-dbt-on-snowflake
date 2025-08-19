select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select PRODUCT_CATEGORY
from INSTRUCTOR1_vending_machine_dbt_db.RAW.PRODUCT_DETAILS
where PRODUCT_CATEGORY is null



      
    ) dbt_internal_test