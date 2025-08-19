select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    VENDING_MACHINE_ID as unique_field,
    count(*) as n_records

from INSTRUCTOR1_vending_machine_dbt_db.RAW.VENDING_MACHINES
where VENDING_MACHINE_ID is not null
group by VENDING_MACHINE_ID
having count(*) > 1



      
    ) dbt_internal_test