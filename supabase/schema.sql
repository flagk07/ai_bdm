-- Ensure required extensions
create extension if not exists pgcrypto;

-- Sequence for unique agent numbers
create sequence if not exists agent_number_seq start 1 increment 1;

-- Allowed users (who can register)
create table if not exists allowed_users (
  tg_id bigint primary key,
  active boolean not null default true,
  created_at timestamptz not null default now()
);

-- Employees registered in bot
create table if not exists employees (
  id uuid primary key default gen_random_uuid(),
  tg_id bigint unique not null,
  agent_number int unique not null default nextval('agent_number_seq'),
  agent_name text generated always as ('agent' || agent_number) stored,
  active boolean not null default true,
  created_at timestamptz not null default now()
);

-- Sales attempts
create table if not exists attempts (
  id uuid primary key default gen_random_uuid(),
  tg_id bigint not null references employees(tg_id) on delete cascade,
  product_code text not null check (product_code in ('КН','КСП','ПУ','ДК','ИК','ИЗП','НС','Вклад','КН к ЗП')),
  attempt_count int not null check (attempt_count >= 0),
  for_date date not null,
  created_at timestamptz not null default now()
);
create index if not exists idx_attempts_tg_date on attempts (tg_id, for_date);

-- Notes (sanitized)
create table if not exists notes (
  id uuid primary key default gen_random_uuid(),
  tg_id bigint not null references employees(tg_id) on delete cascade,
  content_sanitized text not null,
  created_at timestamptz not null default now()
);
create index if not exists idx_notes_tg_created on notes (tg_id, created_at desc);

-- Assistant messages (sanitized)
create table if not exists assistant_messages (
  id uuid primary key default gen_random_uuid(),
  tg_id bigint not null references employees(tg_id) on delete cascade,
  role text not null check (role in ('system','user','assistant')),
  content_sanitized text not null,
  created_at timestamptz not null default now()
);
create index if not exists idx_assistant_msgs_tg_created on assistant_messages (tg_id, created_at desc);

-- Add off_topic flag to assistant_messages (idempotent)
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'assistant_messages' AND column_name = 'off_topic'
	) THEN
		ALTER TABLE assistant_messages ADD COLUMN off_topic boolean not null default false;
	END IF;
END$$;

-- Add auto flag to assistant_messages (idempotent)
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'assistant_messages' AND column_name = 'auto'
	) THEN
		ALTER TABLE assistant_messages ADD COLUMN auto boolean not null default false;
	END IF;
END$$;

-- Sales plans per agent per month
create table if not exists sales_plans (
  tg_id bigint not null references employees(tg_id) on delete cascade,
  year int not null,
  month int not null check (month between 1 and 12),
  plan_month int not null check (plan_month >= 0) default 200,
  created_at timestamptz not null default now(),
  primary key (tg_id, year, month)
);

-- Logs
create table if not exists logs (
  id uuid primary key default gen_random_uuid(),
  tg_id bigint,
  action text not null,
  payload jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_logs_created on logs (created_at desc);

-- Meet (client meetings by delivered product)
create table if not exists meet (
  id uuid primary key default gen_random_uuid(),
  tg_id bigint not null references employees(tg_id) on delete cascade,
  product_code text not null check (product_code in ('ЗП','ДК','МК','ПУ','КН','ТС','Вклад','ИК','Эскроу','КК','Аккредитив')),
  for_date date not null default current_date,
  created_at timestamptz not null default now()
);
create index if not exists idx_meet_tg_created on meet (tg_id, created_at desc);

-- Add meet_id to attempts (idempotent)
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'attempts' AND column_name = 'meet_id'
	) THEN
		ALTER TABLE attempts ADD COLUMN meet_id uuid references meet(id) on delete set null;
	END IF;
END$$;

-- RLS Policies (MVP: allow anon role to read/write). Adjust for production.
alter table allowed_users enable row level security;
alter table employees enable row level security;
alter table attempts enable row level security;
alter table notes enable row level security;
alter table assistant_messages enable row level security;
alter table logs enable row level security;
alter table sales_plans enable row level security;
alter table meet enable row level security;

-- Permissive policies for anon (for server-side bot only). Use service key in production.
-- Recreate policies idempotently: drop then create

drop policy if exists anon_all_allowed_users on allowed_users;
create policy anon_all_allowed_users on allowed_users for all using (true) with check (true);

drop policy if exists anon_all_employees on employees;
create policy anon_all_employees on employees for all using (true) with check (true);

drop policy if exists anon_all_attempts on attempts;
create policy anon_all_attempts on attempts for all using (true) with check (true);

drop policy if exists anon_all_notes on notes;
create policy anon_all_notes on notes for all using (true) with check (true);

drop policy if exists anon_all_assistant_messages on assistant_messages;
create policy anon_all_assistant_messages on assistant_messages for all using (true) with check (true);

drop policy if exists anon_all_logs on logs;
create policy anon_all_logs on logs for all using (true) with check (true);

-- Permissive policy for bot (anon)
drop policy if exists anon_all_sales_plans on sales_plans;
create policy anon_all_sales_plans on sales_plans for all using (true) with check (true);

drop policy if exists anon_all_meet on meet;
create policy anon_all_meet on meet for all using (true) with check (true); 

-- RAG documents (public product materials)
create table if not exists rag_docs (
  id uuid primary key default gen_random_uuid(),
  url text not null,
  title text,
  product_code text,
  mime text,
  fetched_at timestamptz not null default now(),
  content text not null
);
create index if not exists idx_rag_docs_url on rag_docs (url);
create index if not exists idx_rag_docs_product on rag_docs (product_code);
create index if not exists idx_rag_docs_fetched on rag_docs (fetched_at desc);

-- Ensure unique URL constraint for upsert
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.table_constraints
		WHERE table_name = 'rag_docs' AND constraint_name = 'rag_docs_url_unique'
	) THEN
		ALTER TABLE rag_docs ADD CONSTRAINT rag_docs_url_unique UNIQUE (url);
	END IF;
END$$;

-- Enable RLS for rag_docs and permit anon for server-side bot (same as others)
alter table rag_docs enable row level security;

drop policy if exists anon_all_rag_docs on rag_docs;
create policy anon_all_rag_docs on rag_docs for all using (true) with check (true); 