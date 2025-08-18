-- What is the total revenue per product category?
SELECT
    pd.product_category,
    SUM(vt.transaction_total_amount) AS total_revenue
FROM vending_dbt_db.raw.vending_transactions vt
JOIN vending_dbt_db.raw.product_details pd
    ON vt.product_id = pd.product_id
GROUP BY pd.product_category
ORDER BY total_revenue DESC;

-- What are the top 5 most popular products based on quantity sold?
SELECT
    pd.product_name,
    SUM(vt.quantity) AS total_quantity_sold
FROM vending_dbt_db.raw.vending_transactions vt
JOIN vending_dbt_db.raw.product_details pd
    ON vt.product_id = pd.product_id
GROUP BY pd.product_name
ORDER BY total_quantity_sold DESC
LIMIT 5;

-- Get the average satisfaction score by customer age group
SELECT
    CASE
        WHEN c.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
        ELSE '55+'
    END AS age_group,
    AVG(sf.satisfaction_score) AS average_satisfaction
FROM vending_dbt_db.raw.survey_feedback sf
JOIN vending_dbt_db.raw.customer_details c
    ON sf.customer_id = c.customer_id
GROUP BY age_group
ORDER BY age_group;

