select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select SATISFACTION_SCORE
from INSTRUCTOR1_vending_machine_dbt_db.RAW.SURVEY_FEEDBACK
where SATISFACTION_SCORE is null



      
    ) dbt_internal_test