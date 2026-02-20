-- =========================================================
-- 1) gold.company_daily_activity
-- =========================================================
DROP TABLE IF EXISTS gold.company_daily_activity;

CREATE TABLE gold.company_daily_activity AS
WITH base AS (
  SELECT
    u.company_id,
    e.user_id,
    e.event_type,
    e.event_timestamp::date AS activity_date
  FROM silver.events e
  JOIN silver.users  u ON u.user_id = e.user_id
)
SELECT
  company_id,
  activity_date,
  COUNT(DISTINCT user_id) AS active_users,
  COUNT(*) FILTER (WHERE event_type = 'login')        AS login_count,
  COUNT(*) FILTER (WHERE event_type = 'page_created') AS pages_created,
  COUNT(*) FILTER (WHERE event_type = 'file_uploaded') AS files_uploaded,
  COUNT(*) FILTER (WHERE event_type = 'invite_sent')  AS invites_sent,
  COUNT(*) AS total_events
FROM base
GROUP BY 1, 2;

ALTER TABLE gold.company_daily_activity
  ADD CONSTRAINT pk_company_daily_activity
  PRIMARY KEY (company_id, activity_date);

-- Performance indexes (PK already helps; add date for time filtering)
CREATE INDEX IF NOT EXISTS idx_cda_activity_date
  ON gold.company_daily_activity (activity_date);

-- Sanity checks
-- 1) Any negative/odd values? (should be none)
SELECT *
FROM gold.company_daily_activity
WHERE active_users < 0 OR total_events < 0
LIMIT 10;

-- 2) Quick row count
SELECT COUNT(*) AS rows FROM gold.company_daily_activity;

-- 3) Spot-check
SELECT COUNT(*) FROM gold.company_daily_activity;

SELECT *
FROM gold.company_daily_activity
ORDER BY activity_date DESC
LIMIT 10;

-- =========================================================
-- 2) gold.company_monthly_metrics
-- =========================================================
DROP TABLE IF EXISTS gold.company_monthly_metrics;

CREATE TABLE gold.company_monthly_metrics AS
WITH
activity_m AS (
  SELECT
    company_id,
    date_trunc('month', activity_date)::date AS month_start,
    COUNT(DISTINCT CASE WHEN active_users > 0 THEN company_id END) AS _dummy, -- placeholder for safety
    MAX(active_users) FILTER (WHERE TRUE) AS _ignore, -- placeholder
    -- monthly activity metrics:
    COUNT(DISTINCT CASE WHEN active_users > 0 THEN activity_date END) AS active_days,
    MAX(active_users) AS peak_daily_active_users,
    COUNT(DISTINCT CASE WHEN active_users > 0 THEN NULL END) AS _noop,
    COUNT(DISTINCT CASE WHEN active_users > 0 THEN NULL END) AS _noop2,
    -- monthly unique active users: recompute from daily by max distinct users per month isn't possible from daily.
    -- We'll approximate monthly active users as MAX(daily active users) is wrong.
    -- Better: recompute from raw events:
    0::int AS monthly_active_users_placeholder
  FROM gold.company_daily_activity
  GROUP BY 1, 2
),
mau AS (
  SELECT
    u.company_id,
    date_trunc('month', e.event_timestamp)::date AS month_start,
    COUNT(DISTINCT e.user_id) AS monthly_active_users
  FROM silver.events e
  JOIN silver.users u ON u.user_id = e.user_id
  GROUP BY 1, 2
),
activity_sums AS (
  SELECT
    company_id,
    date_trunc('month', activity_date)::date AS month_start,
    SUM(total_events)    AS total_events,
    SUM(login_count)     AS total_logins,
    SUM(invites_sent)    AS total_invites,
    SUM(files_uploaded)  AS total_files,
    SUM(pages_created)   AS total_pages_created
  FROM gold.company_daily_activity
  GROUP BY 1, 2
),
tickets AS (
  SELECT
    company_id,
    date_trunc('month', created_at)::date AS month_start,
    COUNT(*) AS tickets_count
  FROM silver.support_tickets
  GROUP BY 1, 2
),
failed_payments AS (
  SELECT
    company_id,
    date_trunc('month', payment_date)::date AS month_start,
    COUNT(*) FILTER (WHERE payment_status IN ('failed','declined')) AS failed_payments
  FROM silver.payments
  GROUP BY 1, 2
),
-- build a month spine per company so missing months still exist
as_of AS (
  SELECT GREATEST(
    COALESCE((SELECT MAX(event_timestamp)::date FROM silver.events), DATE '2000-01-01'),
    COALESCE((SELECT MAX(payment_date)::date     FROM silver.payments), DATE '2000-01-01'),
    COALESCE((SELECT MAX(COALESCE(end_date, start_date)) FROM silver.subscriptions), DATE '2000-01-01')
  ) AS as_of_date
),
company_month_spine AS (
  SELECT
    c.company_id,
    gs::date AS month_start
  FROM silver.companies c
  CROSS JOIN as_of
  CROSS JOIN LATERAL generate_series(
    date_trunc('month', c.signup_date)::date,
    date_trunc('month', as_of.as_of_date)::date,
    interval '1 month'
  ) gs
)
SELECT
  s.company_id,
  s.month_start,
  COALESCE(mau.monthly_active_users, 0) AS active_users_monthly,
  COALESCE(a.total_events, 0)    AS total_events,
  COALESCE(a.total_logins, 0)    AS total_logins,
  COALESCE(a.total_invites, 0)   AS total_invites,
  COALESCE(a.total_files, 0)     AS total_files,
  COALESCE(a.total_pages_created, 0) AS total_pages_created,
  COALESCE(t.tickets_count, 0)   AS tickets_count,
  COALESCE(fp.failed_payments, 0) AS failed_payments
FROM company_month_spine s
LEFT JOIN activity_sums a
  ON a.company_id = s.company_id AND a.month_start = s.month_start
LEFT JOIN mau
  ON mau.company_id = s.company_id AND mau.month_start = s.month_start
LEFT JOIN tickets t
  ON t.company_id = s.company_id AND t.month_start = s.month_start
LEFT JOIN failed_payments fp
  ON fp.company_id = s.company_id AND fp.month_start = s.month_start;

ALTER TABLE gold.company_monthly_metrics
  ADD CONSTRAINT pk_company_monthly_metrics
  PRIMARY KEY (company_id, month_start);

CREATE INDEX IF NOT EXISTS idx_cmm_month_start
  ON gold.company_monthly_metrics (month_start);

-- Sanity checks
SELECT COUNT(*) AS rows FROM gold.company_monthly_metrics;

-- Check missing months exist (should be 0 rows if spine worked)
SELECT *
FROM gold.company_monthly_metrics
WHERE month_start IS NULL
LIMIT 10;

-- Spot check: does active_users_monthly align with any activity?
SELECT *
FROM gold.company_monthly_metrics
WHERE active_users_monthly = 0 AND total_events > 0
LIMIT 20;

-- =========================================================
-- 3) gold.company_mrr
-- =========================================================
DROP TABLE IF EXISTS gold.company_mrr;

CREATE TABLE gold.company_mrr AS
WITH
as_of AS (
  SELECT GREATEST(
    COALESCE((SELECT MAX(event_timestamp)::date FROM silver.events), DATE '2000-01-01'),
    COALESCE((SELECT MAX(payment_date)::date     FROM silver.payments), DATE '2000-01-01'),
    COALESCE((SELECT MAX(COALESCE(end_date, start_date)) FROM silver.subscriptions), DATE '2000-01-01')
  ) AS as_of_date
),
company_month_spine AS (
  SELECT
    c.company_id,
    gs::date AS month_start,
    (gs::date + INTERVAL '1 month' - INTERVAL '1 day')::date AS month_end
  FROM silver.companies c
  CROSS JOIN as_of
  CROSS JOIN LATERAL generate_series(
    date_trunc('month', c.signup_date)::date,
    date_trunc('month', as_of.as_of_date)::date,
    interval '1 month'
  ) gs
),
mau AS (
  SELECT
    u.company_id,
    date_trunc('month', e.event_timestamp)::date AS month_start,
    COUNT(DISTINCT e.user_id) AS active_user_count
  FROM silver.events e
  JOIN silver.users u ON u.user_id = e.user_id
  GROUP BY 1, 2
),
current_plan AS (
  SELECT
    cms.company_id,
    cms.month_start,
    sp.plan_id,
    sp.plan_name,
    sp.price_per_user,
    sp.billing_cycle
  FROM company_month_spine cms
  LEFT JOIN LATERAL (
    SELECT s.*
    FROM silver.subscriptions s
    WHERE s.company_id = cms.company_id
      AND s.start_date <= cms.month_end
      AND (s.end_date IS NULL OR s.end_date >= cms.month_start)
      AND s.status IN ('active','cancelled')  -- include cancelled overlaps; change if you want only active
    ORDER BY s.start_date DESC
    LIMIT 1
  ) sub ON TRUE
  LEFT JOIN silver.subscription_plans sp
    ON sp.plan_id = sub.plan_id
)
SELECT
  cms.company_id,
  cms.month_start,
  COALESCE(cp.plan_id, 'unknown') AS plan_id,
  COALESCE(cp.plan_name, 'unknown') AS plan_name,
  COALESCE(cp.price_per_user, 0)::numeric AS price_per_user,
  COALESCE(mau.active_user_count, 0) AS active_user_count,
  (COALESCE(mau.active_user_count, 0) * COALESCE(cp.price_per_user, 0))::numeric AS mrr
FROM company_month_spine cms
LEFT JOIN mau
  ON mau.company_id = cms.company_id AND mau.month_start = cms.month_start
LEFT JOIN current_plan cp
  ON cp.company_id = cms.company_id AND cp.month_start = cms.month_start;

ALTER TABLE gold.company_mrr
  ADD CONSTRAINT pk_company_mrr
  PRIMARY KEY (company_id, month_start);

CREATE INDEX IF NOT EXISTS idx_company_mrr_month
  ON gold.company_mrr (month_start);

CREATE INDEX IF NOT EXISTS idx_company_mrr_plan
  ON gold.company_mrr (plan_id);

-- Sanity checks
SELECT
  MIN(month_start) AS min_month,
  MAX(month_start) AS max_month,
  SUM(mrr) AS total_mrr
FROM gold.company_mrr;

-- Any negative MRR? should be none
SELECT *
FROM gold.company_mrr
WHERE mrr < 0
LIMIT 50;

-- =========================================================
-- 4) gold.company_churn_label
-- =========================================================
DROP TABLE IF EXISTS gold.company_churn_label;

CREATE TABLE gold.company_churn_label AS
WITH
as_of AS (
  SELECT GREATEST(
    COALESCE((SELECT MAX(event_timestamp)::date FROM silver.events), DATE '2000-01-01'),
    COALESCE((SELECT MAX(payment_date)::date     FROM silver.payments), DATE '2000-01-01'),
    COALESCE((SELECT MAX(COALESCE(end_date, start_date)) FROM silver.subscriptions), DATE '2000-01-01')
  ) AS as_of_date
),
last_activity AS (
  SELECT
    u.company_id,
    MAX(e.event_timestamp)::date AS last_activity_date
  FROM silver.events e
  JOIN silver.users u ON u.user_id = e.user_id
  GROUP BY 1
),
cancelled AS (
  SELECT
    company_id,
    MIN(COALESCE(end_date, start_date))::date AS cancelled_date
  FROM silver.subscriptions
  WHERE status = 'cancelled'
  GROUP BY 1
),
active_asof AS (
  SELECT
    c.company_id,
    EXISTS (
      SELECT 1
      FROM silver.subscriptions s
      CROSS JOIN as_of
      WHERE s.company_id = c.company_id
        AND s.start_date <= as_of.as_of_date
        AND (s.end_date IS NULL OR s.end_date >= as_of.as_of_date)
        AND s.status IN ('active')
    ) AS has_active_subscription_asof
  FROM silver.companies c
),
base AS (
  SELECT
    c.company_id,
    la.last_activity_date,
    can.cancelled_date,
    aao.has_active_subscription_asof,
    as_of.as_of_date
  FROM silver.companies c
  CROSS JOIN as_of
  LEFT JOIN last_activity la ON la.company_id = c.company_id
  LEFT JOIN cancelled    can ON can.company_id = c.company_id
  LEFT JOIN active_asof  aao ON aao.company_id = c.company_id
)
SELECT
  company_id,
  CASE
    WHEN cancelled_date IS NOT NULL THEN TRUE
    WHEN last_activity_date IS NOT NULL
         AND last_activity_date <= (as_of_date - INTERVAL '30 days')::date
         AND has_active_subscription_asof = FALSE
      THEN TRUE
    ELSE FALSE
  END AS churn_flag,
  CASE
    WHEN cancelled_date IS NOT NULL THEN cancelled_date
    WHEN last_activity_date IS NOT NULL
         AND last_activity_date <= (as_of_date - INTERVAL '30 days')::date
         AND has_active_subscription_asof = FALSE
      THEN (last_activity_date + INTERVAL '30 days')::date
    ELSE NULL::date
  END AS churn_date,
  CASE
    WHEN cancelled_date IS NOT NULL THEN 'cancelled'
    WHEN last_activity_date IS NOT NULL
         AND last_activity_date <= (as_of_date - INTERVAL '30 days')::date
         AND has_active_subscription_asof = FALSE
      THEN 'inactive_30d'
    ELSE 'active'
  END AS churn_reason,
  last_activity_date,
  as_of_date
FROM base;

ALTER TABLE gold.company_churn_label
  ADD CONSTRAINT pk_company_churn_label
  PRIMARY KEY (company_id);

CREATE INDEX IF NOT EXISTS idx_churn_flag
  ON gold.company_churn_label (churn_flag);

-- Sanity checks
SELECT churn_reason, COUNT(*) AS companies
FROM gold.company_churn_label
GROUP BY 1
ORDER BY 2 DESC;

-- Ensure churn_date exists when churn_flag is true (ideally 0 rows)
SELECT *
FROM gold.company_churn_label
WHERE churn_flag = TRUE AND churn_date IS NULL
LIMIT 50;



