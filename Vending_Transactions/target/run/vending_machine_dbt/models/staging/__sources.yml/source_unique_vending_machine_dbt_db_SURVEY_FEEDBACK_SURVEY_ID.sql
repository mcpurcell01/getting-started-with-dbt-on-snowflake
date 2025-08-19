select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    SURVEY_ID as unique_field,
    count(*) as n_records

from INSTRUCTOR1_vending_machine_dbt_db.RAW.SURVEY_FEEDBACK
where SURVEY_ID is not null
group by SURVEY_ID
having count(*) > 1



      
    ) dbt_internal_test