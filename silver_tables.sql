-- silver.companies definition

-- Drop table

-- DROP TABLE silver.companies;

CREATE TABLE silver.companies (
	company_id text NOT NULL,
	company_name text NULL,
	signup_date date NULL,
	acquisition_channel text NULL,
	industry text NULL,
	company_size int4 NULL,
	country text NULL,
	status text NULL,
	company_size_bucket text NULL,
	CONSTRAINT pk_silver_companies PRIMARY KEY (company_id)
);

-- silver.events definition

-- Drop table

-- DROP TABLE silver.events;

CREATE TABLE silver.events (
	event_id text NOT NULL,
	user_id text NULL,
	event_type text NULL,
	event_timestamp timestamp NULL,
	CONSTRAINT pk_silver_events PRIMARY KEY (event_id)
);
CREATE INDEX idx_silver_events_user_ts ON silver.events USING btree (user_id, event_timestamp);


-- silver.events foreign keys

ALTER TABLE silver.events ADD CONSTRAINT fk_events_user FOREIGN KEY (user_id) REFERENCES silver.users(user_id);

-- silver.payments definition

-- Drop table

-- DROP TABLE silver.payments;

CREATE TABLE silver.payments (
	payment_id text NOT NULL,
	company_id text NULL,
	amount numeric NULL,
	payment_date date NULL,
	payment_status text NULL,
	CONSTRAINT pk_silver_payments PRIMARY KEY (payment_id)
);
CREATE INDEX idx_silver_payments_company_date ON silver.payments USING btree (company_id, payment_date);


-- silver.payments foreign keys

ALTER TABLE silver.payments ADD CONSTRAINT fk_payments_company FOREIGN KEY (company_id) REFERENCES silver.companies(company_id);

-- silver.subscription_plans definition

-- Drop table

-- DROP TABLE silver.subscription_plans;

CREATE TABLE silver.subscription_plans (
	plan_id text NOT NULL,
	plan_name text NULL,
	price_per_user numeric NULL,
	billing_cycle text NULL,
	CONSTRAINT pk_silver_subscription_plans PRIMARY KEY (plan_id)
);

-- silver.subscriptions definition

-- Drop table

-- DROP TABLE silver.subscriptions;

CREATE TABLE silver.subscriptions (
	subscription_id text NOT NULL,
	company_id text NULL,
	plan_id text NULL,
	start_date date NULL,
	end_date date NULL,
	status text NULL,
	CONSTRAINT pk_silver_subscriptions PRIMARY KEY (subscription_id)
);
CREATE INDEX idx_silver_subscriptions_company_dates ON silver.subscriptions USING btree (company_id, start_date, end_date);


-- silver.subscriptions foreign keys

ALTER TABLE silver.subscriptions ADD CONSTRAINT fk_subs_company FOREIGN KEY (company_id) REFERENCES silver.companies(company_id);
ALTER TABLE silver.subscriptions ADD CONSTRAINT fk_subs_plan FOREIGN KEY (plan_id) REFERENCES silver.subscription_plans(plan_id);

-- silver.support_tickets definition

-- Drop table

-- DROP TABLE silver.support_tickets;

CREATE TABLE silver.support_tickets (
	ticket_id text NOT NULL,
	company_id text NULL,
	created_at timestamp NULL,
	resolution_time_hours numeric NULL,
	satisfaction_score int4 NULL,
	CONSTRAINT pk_silver_support_tickets PRIMARY KEY (ticket_id)
);
CREATE INDEX idx_silver_tickets_company_date ON silver.support_tickets USING btree (company_id, created_at);


-- silver.support_tickets foreign keys

ALTER TABLE silver.support_tickets ADD CONSTRAINT fk_tickets_company FOREIGN KEY (company_id) REFERENCES silver.companies(company_id);

-- silver.users definition

-- Drop table

-- DROP TABLE silver.users;

CREATE TABLE silver.users (
	user_id text NOT NULL,
	company_id text NULL,
	"role" text NULL,
	join_date date NULL,
	last_active_date date NULL,
	is_active bool NULL,
	CONSTRAINT pk_users PRIMARY KEY (user_id)
);


-- silver.users foreign keys

ALTER TABLE silver.users ADD CONSTRAINT fk_users_company FOREIGN KEY (company_id) REFERENCES silver.companies(company_id);