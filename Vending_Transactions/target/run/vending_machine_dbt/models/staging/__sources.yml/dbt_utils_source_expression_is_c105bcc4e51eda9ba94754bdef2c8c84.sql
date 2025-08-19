select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      



select
    1
from INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_TRANSACTIONS

where not(TRANSACTION_TIMESTAMP  <= current_timestamp())


      
    ) dbt_internal_test