# SaaS Churn Risk & Revenue Exposure Analytics System

End-to-end B2B SaaS churn probability modeling and revenue-at-risk monitoring pipeline  
(PostgreSQL → Feature Engineering → ML → Tableau)

<img width="1199" height="799" alt="Revenue Risk Drivers" src="https://github.com/user-attachments/assets/5078cee1-8394-4cce-8a65-bae7ea1d42b3" />

---

## 📌 Project Overview

This project simulates a real SaaS analytics workflow designed to:

- Predict churn probability at the company level  
- Quantify financial exposure using revenue-at-risk modeling  
- Identify behavioral and lifecycle risk drivers  
- Deliver executive-ready dashboards for monitoring  

The result is a churn early-warning system that connects **product engagement signals to financial impact**.

---

## 🎯 Business Questions Answered

1. What percentage of MRR is currently exposed to churn risk?
2. Which plan tiers contribute the largest revenue exposure?
3. Is exposure concentrated in specific customer segments?
4. Are at-risk accounts low-, medium-, or high-engagement?
5. Is churn risk driven by onboarding issues or mid-lifecycle plateau?

---

## 🏗 Data Architecture (Medallion Model)

This project follows a structured raw → silver → gold architecture.

### 🔹 RAW Schema (Ingestion Layer)

Unmodified CSV ingestion.

Tables:
- companies
- users
- events
- subscriptions
- subscription_plans
- payments
- support_tickets

Raw data preserves source formatting for traceability.

---

### 🔹 SILVER Schema (Cleaned & Standardized)

<img width="776" height="1008" alt="silver_schema" src="https://github.com/user-attachments/assets/f6b9ffe3-da38-4938-a1e4-501a0721d336" />

Cleaning and normalization steps included:

- Standardizing timestamp formats
- Converting text-based numerics to numeric types
- Replacing empty strings with NULL
- Handling invalid or malformed values
- Enforcing consistent company_id relationships
- Removing duplicate records
- Structuring monthly snapshot data

Example transformation:
- `satisfaction_score` stored as text in raw
- Converted to integer in silver
- Empty strings → NULL
- Invalid values removed

This layer ensures analytical consistency before feature engineering.

---

### 🔹 GOLD Schema (Business-Ready Layer)

Aggregated, model-ready, and dashboard-ready tables.

<img width="2058" height="723" alt="gold_schema" src="https://github.com/user-attachments/assets/e441242a-c1f6-4225-b9e7-32e645749f78" />

Key tables:

- `company_features_for_model`
- `company_churn_predictions`
- `company_revenue_at_risk`
- `company_risk_monitor`
- `company_monthly_metrics`
- `company_ltv`
- `retention_company_cohorts`

Final monitoring dataset:
`gold.company_risk_monitor`

Fields include:
- company_id
- month_start
- plan_type
- months_since_signup
- logins_30d
- failed_payments_60d
- mrr
- churn_probability
- revenue_at_risk

---

## 🧠 Feature Engineering

Monthly company-level features were created including:

- Active users (30-day window)
- Login frequency (30-day window)
- Upload activity
- Support tickets (60-day window)
- Failed payments (billing risk)
- Tenure (months since signup)
- Plan tier
- Static churn label
- Churn in next 30 days (model target)

This creates a structured feature table for churn modeling.

---

## 🤖 Modeling Approach

- Model Type: Logistic Regression
- Target Variable: `churn_in_next_30d`
- Output: `churn_probability` per company-month

Revenue at Risk calculation:

```
revenue_at_risk = mrr × churn_probability
```

This directly translates model output into financial exposure.

---

## Model Evaluation

The model demonstrates strong ranking capability (ROC-AUC 0.84), making it suitable for risk-based prioritization rather than binary classification alone.
The model was evaluated using a time-based split 
(pre-2024-09 training, post-2024-09 testing).

- ROC-AUC: 0.84
- Test churn rate: 0.29%
- Top 20% intervention captures ~63% of churners (≈3× lift)

<img width="712" height="698" alt="Screenshot 2026-02-21 044204" src="https://github.com/user-attachments/assets/d35f186e-1823-40a9-9afd-3976f33c69f1" />

---

## 📊 Dashboard Overview (Tableau)

### 1️⃣ Executive Overview

- Total MRR
- Revenue at Risk
- % MRR Exposed
- Churn Risk Distribution
- MRR Trend

---

### 2️⃣ Revenue Risk Drivers

- Churn Risk vs Product Engagement (Scatter)
- Revenue at Risk by Usage Tier
- Revenue at Risk by Customer Tenure
- Plan-Level Exposure Segmentation

---

### 3️⃣ Account Monitoring (Optional View)

- Top At-Risk Accounts
- Portfolio Concentration Analysis
- Interactive Filters

---

## 🔎 Key Insights

- Revenue exposure is concentrated in the **Pro plan tier** in absolute terms.
- Risk is disproportionately concentrated among **medium-engagement customers**, indicating incomplete value realization.
- Revenue at risk peaks in **mid-to-late tenure (18–30 months)** customers.
- Risk appears driven by engagement plateau rather than early onboarding failure.

---

## 💡 Strategic Implications

- Prioritize expansion campaigns for medium-engagement accounts.
- Target mid-tenure customers before renewal cycles.
- Implement retention playbooks based on MRR × risk level.
- Use revenue-at-risk ranking to prioritize CSM outreach.

---

## 🔁 Reproducibility Steps

1. Load raw CSV files into PostgreSQL (`raw` schema).
2. Execute cleaning and transformation scripts → `silver` schema.
3. Generate feature tables and business views → `gold` schema.
4. Train churn model and generate probabilities.
5. Calculate revenue at risk.
6. Export `gold.company_risk_monitor` to Tableau.
7. Build dashboards.

---

## 🛠 Tech Stack

- PostgreSQL (raw / silver / gold schemas)
- Python (pandas, scikit-learn, SQLAlchemy)
- Tableau
- SQL (CTEs, aggregations, window functions)

---

## 📁 Repository Structure

```
sql/
notebooks/
assets/
README.md
```

---

## 📌 Why This Project Matters

This project demonstrates:

- Data cleaning & transformation at scale
- Structured analytics pipeline design
- Churn modeling with financial translation
- Portfolio-level risk segmentation
- Executive storytelling via dashboards

It bridges data engineering, applied machine learning, and executive decision support in a production-style analytics workflow.
