-- raw.companies definition

-- Drop table

-- DROP TABLE raw.companies;

CREATE TABLE raw.companies (
	company_id text NULL,
	company_name text NULL,
	signup_date date NULL,
	acquisition_channel text NULL,
	industry text NULL,
	company_size int4 NULL,
	country text NULL,
	status text NULL
);

-- raw.events definition

-- Drop table

-- DROP TABLE raw.events;

CREATE TABLE raw.events (
	event_id text NULL,
	user_id text NULL,
	event_type text NULL,
	event_timestamp timestamp NULL
);

-- raw.payments definition

-- Drop table

-- DROP TABLE raw.payments;

CREATE TABLE raw.payments (
	payment_id text NULL,
	company_id text NULL,
	amount numeric NULL,
	payment_date date NULL,
	payment_status text NULL
);

-- raw.subscription_plans definition

-- Drop table

-- DROP TABLE raw.subscription_plans;

CREATE TABLE raw.subscription_plans (
	plan_id text NULL,
	plan_name text NULL,
	price_per_user numeric NULL,
	billing_cycle text NULL
);

-- raw.subscriptions definition

-- Drop table

-- DROP TABLE raw.subscriptions;

CREATE TABLE raw.subscriptions (
	subscription_id text NULL,
	company_id text NULL,
	plan_id text NULL,
	start_date text NULL,
	end_date text NULL,
	status text NULL
);

-- raw.support_tickets definition

-- Drop table

-- DROP TABLE raw.support_tickets;

CREATE TABLE raw.support_tickets (
	ticket_id text NULL,
	company_id text NULL,
	created_at timestamp NULL,
	resolution_time_hours numeric NULL,
	satisfaction_score int4 NULL
);

-- raw.users definition

-- Drop table

-- DROP TABLE raw.users;

CREATE TABLE raw.users (
	user_id text NULL,
	company_id text NULL,
	"role" text NULL,
	join_date text NULL,
	last_active_date text NULL,
	is_active text NULL
);