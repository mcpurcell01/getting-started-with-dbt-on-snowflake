
    
    

select
    SURVEY_ID as unique_field,
    count(*) as n_records

from INSTRUCTOR1_vending_machine_dbt_db.RAW.SURVEY_FEEDBACK
where SURVEY_ID is not null
group by SURVEY_ID
having count(*) > 1


