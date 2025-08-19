
  create or replace   view INSTRUCTOR2_vending_machine_dbt_db.dev.raw_survey_feedback
  
   as (
    -- This staging model selects all columns from the raw survey feedback table.
SELECT
    survey_id,
    transaction_id,
    customer_id,
    satisfaction_score
FROM INSTRUCTOR1_vending_machine_dbt_db.RAW.SURVEY_FEEDBACK
  );

