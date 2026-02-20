-- Orphan users (should return 0)
SELECT COUNT(*) AS orphan_users
FROM silver.users u
LEFT JOIN silver.companies c ON c.company_id = u.company_id
WHERE c.company_id IS NULL;

-- Orphan subscriptions (should return 0)
SELECT COUNT(*) AS orphan_subscriptions
FROM silver.subscriptions s
LEFT JOIN silver.companies c ON c.company_id = s.company_id
WHERE c.company_id IS NULL;

-- Orphan subscriptions to plan (should return 0)
SELECT COUNT(*) AS orphan_subscription_plans
FROM silver.subscriptions s
LEFT JOIN silver.subscription_plans p ON p.plan_id = s.plan_id
WHERE p.plan_id IS NULL;

-- Orphan events (should return 0)
SELECT COUNT(*) AS orphan_events
FROM silver.events e
LEFT JOIN silver.users u ON u.user_id = e.user_id
WHERE u.user_id IS NULL;

-- Orphan payments (should return 0)
SELECT COUNT(*) AS orphan_payments
FROM silver.payments p
LEFT JOIN silver.companies c ON c.company_id = p.company_id
WHERE c.company_id IS NULL;

-- Orphan tickets (should return 0)
SELECT COUNT(*) AS orphan_tickets
FROM silver.support_tickets t
LEFT JOIN silver.companies c ON c.company_id = t.company_id
WHERE c.company_id IS NULL;

-- Example: SILVER critical nulls
SELECT
  SUM(CASE WHEN company_id IS NULL THEN 1 ELSE 0 END) AS null_company_id,
  SUM(CASE WHEN company_name IS NULL THEN 1 ELSE 0 END) AS null_company_name,
  COUNT(*) AS total
FROM silver.companies;

-- GOLD churn label completeness
SELECT
  SUM(CASE WHEN churn_flag IS NULL THEN 1 ELSE 0 END) AS null_churn_flag,
  COUNT(*) AS total
FROM gold.company_churn_label;

-- No negative MRR (should be 0 rows)
SELECT *
FROM gold.company_mrr
WHERE mrr < 0;

-- Active users cannot be negative (should be 0 rows)
SELECT *
FROM gold.company_mrr
WHERE active_user_count < 0;

-- Plan price cannot be negative (should be 0 rows)
SELECT *
FROM gold.company_mrr
WHERE price_per_user < 0;

-- churn_flag = 0 should not have churn_date
SELECT COUNT(*)
FROM gold.company_churn_label
WHERE churn_flag = TRUE
AND churn_date IS NULL;


-- churn_flag = 1 should have churn_date
SELECT COUNT(*) AS bad_rows
FROM gold.company_churn_label
WHERE churn_flag = TRUE AND churn_date IS NULL;

-- churn_reason should be populated when churn_flag=1 (if you enforce this)
SELECT COUNT(*) AS bad_rows
FROM gold.company_churn_label
WHERE churn_flag = TRUE AND (churn_reason IS NULL OR churn_reason = '');


