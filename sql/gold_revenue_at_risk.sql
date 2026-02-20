DROP TABLE IF EXISTS gold.company_revenue_at_risk;

CREATE TABLE gold.company_revenue_at_risk AS
WITH risk_monthly AS (
  SELECT
      company_id,
      date_trunc('month', as_of_date)::date AS month_start,
      churn_probability,
      mrr,
      revenue_at_risk,
      ROW_NUMBER() OVER (
        PARTITION BY company_id, date_trunc('month', as_of_date)::date
        ORDER BY as_of_date DESC
      ) AS rn
  FROM gold.revenue_at_risk
)
SELECT
    m.company_id,
    m.month_start,
    m.mrr,
    COALESCE(rm.churn_probability, 0) AS churn_probability,
    COALESCE(rm.churn_probability, 0) * m.mrr AS revenue_at_risk
FROM gold.company_mrr m
LEFT JOIN risk_monthly rm
  ON m.company_id = rm.company_id
 AND m.month_start = rm.month_start
 AND rm.rn = 1;



SELECT month_start, COUNT(DISTINCT company_id)
FROM gold.company_revenue_at_risk
GROUP BY month_start
ORDER BY month_start;