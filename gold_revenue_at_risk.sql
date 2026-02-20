DROP TABLE IF EXISTS gold.revenue_at_risk;

CREATE TABLE gold.revenue_at_risk AS
SELECT
    p.company_id,
    p.as_of_date,
    p.churn_probability,
    m.mrr,
    (p.churn_probability * m.mrr) AS revenue_at_risk
FROM gold.company_churn_predictions p
LEFT JOIN gold.company_mrr m
    ON m.company_id = p.company_id
   AND m.month_start = date_trunc('month', p.as_of_date);


SELECT *
FROM gold.revenue_at_risk
ORDER BY revenue_at_risk DESC
LIMIT 20;
