-- =========================================================
-- Phase 3A) gold.funnel_company
-- =========================================================
DROP TABLE IF EXISTS gold.funnel_company;

CREATE TABLE gold.funnel_company AS
WITH
signup AS (
  SELECT company_id, signup_date::date AS signup_date, acquisition_channel
  FROM silver.companies
),
activation AS (
  SELECT
    s.company_id,
    EXISTS (
      SELECT 1
      FROM silver.users u
      JOIN silver.events e ON e.user_id = u.user_id
      WHERE u.company_id = s.company_id
        AND e.event_timestamp::date BETWEEN s.signup_date AND (s.signup_date + INTERVAL '7 days')::date
      LIMIT 1
    ) AS activated_7d
  FROM signup s
),
paid AS (
  SELECT
    s.company_id,
    EXISTS (
      SELECT 1
      FROM silver.subscriptions sub
      WHERE sub.company_id = s.company_id
        AND sub.start_date BETWEEN s.signup_date AND (s.signup_date + INTERVAL '30 days')::date
      LIMIT 1
    ) AS paid_30d
  FROM signup s
)
SELECT
  s.company_id,
  s.signup_date,
  s.acquisition_channel,
  a.activated_7d,
  p.paid_30d,
  (a.activated_7d::int) AS activation_stage,
  (p.paid_30d::int)     AS paid_stage
FROM signup s
JOIN activation a USING (company_id)
JOIN paid p USING (company_id);

ALTER TABLE gold.funnel_company
  ADD CONSTRAINT pk_funnel_company PRIMARY KEY (company_id);

CREATE INDEX IF NOT EXISTS idx_funnel_channel ON gold.funnel_company (acquisition_channel);

-- Funnel summary by channel
SELECT
  acquisition_channel,
  COUNT(*) AS signups,
  SUM(activated_7d::int) AS activated,
  SUM(paid_30d::int) AS paid,
  ROUND(AVG(activated_7d::int)::numeric, 4) AS activation_rate,
  ROUND(AVG(paid_30d::int)::numeric, 4)     AS paid_rate
FROM gold.funnel_company
GROUP BY 1
ORDER BY signups DESC;

-- =========================================================
-- Phase 3B) gold.retention_company_cohorts
-- =========================================================
DROP TABLE IF EXISTS gold.retention_company_cohorts;

CREATE TABLE gold.retention_company_cohorts AS
WITH
cohorts AS (
  SELECT
    company_id,
    date_trunc('month', signup_date)::date AS cohort_month
  FROM silver.companies
),
activity AS (
  SELECT
    company_id,
    month_start,
    CASE WHEN active_users_monthly > 0 THEN 1 ELSE 0 END AS retained
  FROM gold.company_monthly_metrics
),
joined AS (
  SELECT
    c.cohort_month,
    a.company_id,
    a.month_start,
    (DATE_PART('year', a.month_start) - DATE_PART('year', c.cohort_month)) * 12
      + (DATE_PART('month', a.month_start) - DATE_PART('month', c.cohort_month)) AS cohort_age_months,
    a.retained
  FROM cohorts c
  JOIN activity a ON a.company_id = c.company_id
  WHERE a.month_start >= c.cohort_month
)
SELECT
  cohort_month,
  cohort_age_months::int AS cohort_age_months,
  COUNT(*) AS companies_in_cell,
  SUM(retained) AS retained_companies,
  ROUND(SUM(retained)::numeric / NULLIF(COUNT(*),0), 4) AS retention_rate
FROM joined
GROUP BY 1, 2
ORDER BY 1, 2;

CREATE INDEX IF NOT EXISTS idx_retention_cohort ON gold.retention_company_cohorts (cohort_month, cohort_age_months);

-- =========================================================
-- Phase 3C) gold.mrr_trend_breakdown
-- =========================================================
DROP TABLE IF EXISTS gold.mrr_trend_breakdown;

CREATE TABLE gold.mrr_trend_breakdown AS
SELECT
  m.month_start,
  c.acquisition_channel,
  m.plan_name,
  SUM(m.mrr) AS total_mrr,
  COUNT(DISTINCT m.company_id) AS companies,
  AVG(m.mrr) AS avg_mrr_per_company
FROM gold.company_mrr m
JOIN silver.companies c ON c.company_id = m.company_id
GROUP BY 1, 2, 3;

CREATE INDEX IF NOT EXISTS idx_mrr_trend_month ON gold.mrr_trend_breakdown (month_start);

-- =========================================================
-- Phase 3D) gold.company_ltv
-- =========================================================
DROP TABLE IF EXISTS gold.company_ltv;

CREATE TABLE gold.company_ltv AS
WITH
as_of AS (
  SELECT GREATEST(
    COALESCE((SELECT MAX(event_timestamp)::date FROM silver.events), DATE '2000-01-01'),
    COALESCE((SELECT MAX(payment_date)::date     FROM silver.payments), DATE '2000-01-01'),
    COALESCE((SELECT MAX(COALESCE(end_date, start_date)) FROM silver.subscriptions), DATE '2000-01-01')
  ) AS as_of_date
),
bounds AS (
  SELECT
    m.company_id,
    MIN(m.month_start) AS first_month,
    MAX(m.month_start) AS last_observed_month,
    AVG(m.mrr) AS avg_mrr_lifetime
  FROM gold.company_mrr m
  GROUP BY 1
),
label AS (
  SELECT
    company_id,
    churn_flag,
    churn_date
  FROM gold.company_churn_label
),
end_month AS (
  SELECT
    b.company_id,
    b.first_month,
    b.avg_mrr_lifetime,
    CASE
      WHEN l.churn_flag = TRUE AND l.churn_date IS NOT NULL
        THEN date_trunc('month', l.churn_date)::date
      ELSE date_trunc('month', as_of.as_of_date)::date
    END AS end_month
  FROM bounds b
  JOIN label l ON l.company_id = b.company_id
  CROSS JOIN as_of
)
SELECT
  e.company_id,
  e.first_month,
  e.end_month,
  (
    (DATE_PART('year', e.end_month) - DATE_PART('year', e.first_month)) * 12
    + (DATE_PART('month', e.end_month) - DATE_PART('month', e.first_month))
    + 1
  )::int AS lifetime_months,
  e.avg_mrr_lifetime,
  (e.avg_mrr_lifetime *
   (
    (DATE_PART('year', e.end_month) - DATE_PART('year', e.first_month)) * 12
    + (DATE_PART('month', e.end_month) - DATE_PART('month', e.first_month))
    + 1
   )
  )::numeric AS ltv_est
FROM end_month e;

ALTER TABLE gold.company_ltv
  ADD CONSTRAINT pk_company_ltv PRIMARY KEY (company_id);

-- =========================================================
-- Phase 3E) gold.company_revenue_at_risk
-- =========================================================
DROP TABLE IF EXISTS gold.company_revenue_at_risk;

CREATE TABLE gold.company_revenue_at_risk AS
SELECT
  m.company_id,
  m.month_start,
  m.mrr,
  NULL::numeric AS churn_probability,         -- to be filled by model output later
  NULL::numeric AS revenue_at_risk            -- mrr * churn_probability later
FROM gold.company_mrr m;

CREATE INDEX IF NOT EXISTS idx_rar_month ON gold.company_revenue_at_risk (month_start);

