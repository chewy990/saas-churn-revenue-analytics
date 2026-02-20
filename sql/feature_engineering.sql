-- =========================================================
-- Phase 4) gold.company_features_for_model
-- =========================================================
DROP TABLE IF EXISTS gold.company_features_for_model;

CREATE TABLE gold.company_features_for_model AS
WITH
-- month_end spine per company
as_of_global AS (
  SELECT GREATEST(
    COALESCE((SELECT MAX(event_timestamp)::date FROM silver.events), DATE '2000-01-01'),
    COALESCE((SELECT MAX(payment_date)::date     FROM silver.payments), DATE '2000-01-01'),
    COALESCE((SELECT MAX(COALESCE(end_date, start_date)) FROM silver.subscriptions), DATE '2000-01-01')
  ) AS as_of_date
),
company_month_end AS (
  SELECT
    c.company_id,
    (gs::date + INTERVAL '1 month' - INTERVAL '1 day')::date AS as_of_date
  FROM silver.companies c
  CROSS JOIN as_of_global g
  CROSS JOIN LATERAL generate_series(
    date_trunc('month', c.signup_date)::date,
    date_trunc('month', g.as_of_date)::date,
    interval '1 month'
  ) gs
),
-- 30d activity features
events_window AS (
  SELECT
    cme.company_id,
    cme.as_of_date,
    COUNT(DISTINCT e.user_id) FILTER (
      WHERE e.event_timestamp::date BETWEEN (cme.as_of_date - INTERVAL '30 days')::date AND cme.as_of_date
    ) AS active_users_30d,
    COUNT(*) FILTER (
      WHERE e.event_type = 'login'
        AND e.event_timestamp::date BETWEEN (cme.as_of_date - INTERVAL '30 days')::date AND cme.as_of_date
    ) AS logins_30d,
    COUNT(*) FILTER (
      WHERE e.event_type = 'invite_sent'
        AND e.event_timestamp::date BETWEEN (cme.as_of_date - INTERVAL '30 days')::date AND cme.as_of_date
    ) AS invites_30d,
    COUNT(*) FILTER (
      WHERE e.event_type = 'file_uploaded'
        AND e.event_timestamp::date BETWEEN (cme.as_of_date - INTERVAL '30 days')::date AND cme.as_of_date
    ) AS uploads_30d
  FROM company_month_end cme
  LEFT JOIN silver.users u ON u.company_id = cme.company_id
  LEFT JOIN silver.events e ON e.user_id = u.user_id
  GROUP BY 1, 2
),
-- 60d support + payment friction
support_window AS (
  SELECT
    cme.company_id,
    cme.as_of_date,
    COUNT(*) FILTER (
      WHERE t.created_at::date BETWEEN (cme.as_of_date - INTERVAL '60 days')::date AND cme.as_of_date
    ) AS tickets_60d
  FROM company_month_end cme
  LEFT JOIN silver.support_tickets t ON t.company_id = cme.company_id
  GROUP BY 1, 2
),
payment_window AS (
  SELECT
    cme.company_id,
    cme.as_of_date,
    COUNT(*) FILTER (
      WHERE p.payment_status IN ('failed','declined')
        AND p.payment_date::date BETWEEN (cme.as_of_date - INTERVAL '60 days')::date AND cme.as_of_date
    ) AS failed_payments_60d
  FROM company_month_end cme
  LEFT JOIN silver.payments p ON p.company_id = cme.company_id
  GROUP BY 1, 2
),
-- days_to_first_invite (company-level)
first_invite AS (
  SELECT
    u.company_id,
    MIN(e.event_timestamp)::date AS first_invite_date
  FROM silver.events e
  JOIN silver.users u ON u.user_id = e.user_id
  WHERE e.event_type = 'invite_sent'
  GROUP BY 1
),
days_to_invite AS (
  SELECT
    c.company_id,
    CASE
      WHEN fi.first_invite_date IS NULL THEN NULL::int
      ELSE (fi.first_invite_date - c.signup_date::date)
    END AS days_to_first_invite
  FROM silver.companies c
  LEFT JOIN first_invite fi ON fi.company_id = c.company_id
),
-- plan_type as of snapshot date
plan_asof AS (
  SELECT
    cme.company_id,
    cme.as_of_date,
    COALESCE(sp.plan_name, 'unknown') AS plan_type
  FROM company_month_end cme
  LEFT JOIN LATERAL (
    SELECT s.*
    FROM silver.subscriptions s
    WHERE s.company_id = cme.company_id
      AND s.start_date <= cme.as_of_date
      AND (s.end_date IS NULL OR s.end_date >= cme.as_of_date)
    ORDER BY s.start_date DESC
    LIMIT 1
  ) sub ON TRUE
  LEFT JOIN silver.subscription_plans sp ON sp.plan_id = sub.plan_id
),
labels AS (
  SELECT company_id, churn_flag, churn_date
  FROM gold.company_churn_label
)
SELECT
  cme.company_id,
  cme.as_of_date,

  COALESCE(ev.active_users_30d, 0) AS active_users_30d,
  COALESCE(ev.logins_30d, 0)       AS logins_30d,
  COALESCE(ev.invites_30d, 0)      AS invites_30d,
  COALESCE(ev.uploads_30d, 0)      AS uploads_30d,
  COALESCE(sw.tickets_60d, 0)      AS tickets_60d,
  COALESCE(pw.failed_payments_60d, 0) AS failed_payments_60d,

  dti.days_to_first_invite,
  pa.plan_type,

  -- tenure feature (months since signup at this snapshot)
  (
    (DATE_PART('year', cme.as_of_date) - DATE_PART('year', c.signup_date)) * 12
    +
    (DATE_PART('month', cme.as_of_date) - DATE_PART('month', c.signup_date))
  )::int AS months_since_signup,

  -- labels
  l.churn_flag AS churn_flag_static,
  CASE
    WHEN l.churn_date IS NOT NULL
     AND l.churn_date > cme.as_of_date
     AND l.churn_date <= (cme.as_of_date + INTERVAL '30 days')::date
    THEN 1 ELSE 0
  END AS churn_in_next_30d

FROM company_month_end cme
JOIN silver.companies c ON c.company_id = cme.company_id
LEFT JOIN events_window  ev ON ev.company_id = cme.company_id AND ev.as_of_date = cme.as_of_date
LEFT JOIN support_window sw ON sw.company_id = cme.company_id AND sw.as_of_date = cme.as_of_date
LEFT JOIN payment_window pw ON pw.company_id = cme.company_id AND pw.as_of_date = cme.as_of_date
LEFT JOIN days_to_invite dti ON dti.company_id = cme.company_id
LEFT JOIN plan_asof pa ON pa.company_id = cme.company_id AND pa.as_of_date = cme.as_of_date
LEFT JOIN labels l ON l.company_id = cme.company_id;

ALTER TABLE gold.company_features_for_model
  ADD CONSTRAINT pk_company_features_for_model PRIMARY KEY (company_id, as_of_date);

CREATE INDEX IF NOT EXISTS idx_features_asof ON gold.company_features_for_model (as_of_date);

-- Sanity checks (leakage guardrails)
-- 1) If churn_in_next_30d = 1, churn_date should be after as_of_date
SELECT *
FROM gold.company_features_for_model f
JOIN gold.company_churn_label l USING (company_id)
WHERE f.churn_in_next_30d = 1 AND l.churn_date <= f.as_of_date
LIMIT 50;


-- verify --
SELECT
  MIN(months_since_signup) AS min_m,
  MAX(months_since_signup) AS max_m
FROM gold.company_features_for_model;

SELECT months_since_signup, COUNT(*)
FROM gold.company_features_for_model
GROUP BY 1
ORDER BY 1
LIMIT 20;


