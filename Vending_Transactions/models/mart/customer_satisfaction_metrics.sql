-- This model aggregates survey feedback to calculate key customer satisfaction metrics.
-- It joins raw survey feedback with vending transaction data to link feedback to specific events.
SELECT
    sf.customer_id,
    sf.gender,
    sf.age,
    sf.email_address,
    AVG(sf.satisfaction_score) AS average_satisfaction,
    ARRAY_AGG(DISTINCT vt.vending_machine_id) AS visited_machines_array,
    COUNT(DISTINCT sf.survey_id) AS surveys_completed
FROM {{ ref('raw_survey_feedback') }} sf
JOIN {{ ref('raw_vending_transactions') }} vt
    ON sf.transaction_id = vt.transaction_id
GROUP BY sf.customer_id, sf.gender, sf.age, sf.email_address
