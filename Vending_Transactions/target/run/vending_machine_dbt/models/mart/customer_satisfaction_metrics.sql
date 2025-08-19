
  
    

        create or replace transient table INSTRUCTOR2_vending_machine_dbt_db.dev.customer_satisfaction_metrics
         as
        (-- This model aggregates survey feedback to calculate key customer satisfaction metrics.
-- It joins raw survey feedback with vending transaction data to link feedback to specific events.
SELECT
  cd.customer_id,
  cd.gender,
  cd.age,
  cd.email_address,
  AVG(sf.satisfaction_score) AS average_satisfaction,
  ARRAY_AGG (DISTINCT vt.vending_machine_id) AS visited_machines_array,
  COUNT(DISTINCT sf.survey_id) AS surveys_completed
FROM
  		INSTRUCTOR2_vending_machine_dbt_db.dev.raw_survey_feedback AS sf
  JOIN 
		INSTRUCTOR2_vending_machine_dbt_db.dev.raw_vending_transactions AS vt ON sf.transaction_id = vt.transaction_id
  JOIN 
		INSTRUCTOR2_vending_machine_dbt_db.dev.raw_customer_details AS cd ON sf.customer_id = cd.customer_id
GROUP BY
  cd.customer_id,
  cd.gender,
  cd.age,
  cd.email_address
        );
      
  