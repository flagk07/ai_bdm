-- Ensure required extensions
create extension if not exists pgcrypto;
create extension if not exists vector;

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

-- RAG chunks (normalized chunks for retrieval)
create table if not exists rag_chunks (
  id uuid primary key default gen_random_uuid(),
  doc_id uuid not null references rag_docs(id) on delete cascade,
  product_code text,
  chunk_index int not null,
  content text not null,
  created_at timestamptz not null default now()
);
create index if not exists idx_rag_chunks_doc on rag_chunks (doc_id, chunk_index);
create index if not exists idx_rag_chunks_prod on rag_chunks (product_code);

alter table rag_chunks enable row level security;
drop policy if exists anon_all_rag_chunks on rag_chunks;
create policy anon_all_rag_chunks on rag_chunks for all using (true) with check (true);

-- Add currency and embedding to rag_chunks (idempotent)
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'rag_chunks' AND column_name = 'currency'
	) THEN
		ALTER TABLE rag_chunks ADD COLUMN currency text;
	END IF;
END$$;

DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = 'rag_chunks' AND column_name = 'embedding'
	) THEN
		ALTER TABLE rag_chunks ADD COLUMN embedding vector(1536);
	END IF;
END$$;

create index if not exists idx_rag_chunks_currency on rag_chunks (currency);
-- Approximate vector index for cosine distance
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = 'idx_rag_chunks_embedding'
	) THEN
		EXECUTE 'create index idx_rag_chunks_embedding on rag_chunks using ivfflat (embedding vector_cosine_ops) with (lists = 100)';
	END IF;
END$$;

-- RPC for vector search with filters
create or replace function match_rag_chunks(
	product text,
	currency_in text,
	query_embedding vector(1536),
	match_count int
) returns table (
	id uuid,
	doc_id uuid,
	content text,
	product_code text,
	currency text,
	chunk_index int,
	distance float
) language sql stable as $$
  select c.id, c.doc_id, c.content, c.product_code, c.currency, c.chunk_index,
         c.embedding <=> query_embedding as distance
  from rag_chunks c
  where (product is null or c.product_code = product)
    and (currency_in is null or c.currency = currency_in)
    and c.embedding is not null
  order by c.embedding <=> query_embedding
  limit match_count
$$;

-- Normalized product rates for deposits (FACTS)
create table if not exists product_rates (
  id uuid primary key default gen_random_uuid(),
  product_code text not null check (product_code in ('Вклад')),
  payout_type text not null check (payout_type in ('monthly','end')),
  term_days int not null check (term_days in (61,91,122,181,274,367,550,730,1100)),
  amount_min numeric not null,
  amount_max numeric,
  amount_inclusive_end boolean not null default true,
  rate_percent numeric not null check (rate_percent > 0 and rate_percent <= 100),
  channel text,
  effective_from date,
  effective_to date,
  source_url text,
  source_page int,
  created_at timestamptz not null default now()
);
create index if not exists idx_product_rates_prod on product_rates(product_code, payout_type, term_days);
create index if not exists idx_product_rates_eff on product_rates(effective_from desc);

alter table product_rates enable row level security;
 drop policy if exists anon_all_product_rates on product_rates;
create policy anon_all_product_rates on product_rates for all using (true) with check (true); 

-- Relax term_days constraint to allow any positive integer
DO $$
BEGIN
	IF EXISTS (
		SELECT 1 FROM information_schema.table_constraints tc
		JOIN information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
		WHERE tc.table_name = 'product_rates' AND tc.constraint_type = 'CHECK' AND ccu.column_name = 'term_days'
	) THEN
		ALTER TABLE product_rates DROP CONSTRAINT IF EXISTS product_rates_term_days_check;
	END IF;
END$$;

ALTER TABLE product_rates
	ADD CONSTRAINT product_rates_term_days_check CHECK (term_days > 0); 

-- Assistant slots: remember structured parameters per chat (no timeout)
create table if not exists assistant_slots (
  tg_id bigint primary key,
  product_code text,
  payout_type text check (payout_type in ('monthly','end')),
  currency text check (currency in ('RUB','USD','EUR','CNY')),
  term_days int check (term_days > 0),
  amount numeric,
  channel text,
  updated_at timestamptz not null default now()
);
create index if not exists idx_assistant_slots_updated on assistant_slots(updated_at desc);

-- ================= New migrations for FACTS ↔ RAG linkage and generalized facts =================
-- Add doc_id and dates to product_rates (idempotent)
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns WHERE table_name = 'product_rates' AND column_name = 'doc_id'
	) THEN
		ALTER TABLE product_rates ADD COLUMN doc_id uuid;
	END IF;
	IF NOT EXISTS (
		SELECT 1 FROM pg_indexes WHERE indexname = 'idx_product_rates_doc'
	) THEN
		CREATE INDEX idx_product_rates_doc ON product_rates(doc_id);
	END IF;
END$$;

-- Extend rag_chunks with linkage and attributes (idempotent)
DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns WHERE table_name = 'rag_chunks' AND column_name = 'section_path'
	) THEN
		ALTER TABLE rag_chunks ADD COLUMN section_path text;
	END IF;
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns WHERE table_name = 'rag_chunks' AND column_name = 'effective_from'
	) THEN
		ALTER TABLE rag_chunks ADD COLUMN effective_from date;
	END IF;
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns WHERE table_name = 'rag_chunks' AND column_name = 'effective_to'
	) THEN
		ALTER TABLE rag_chunks ADD COLUMN effective_to date;
	END IF;
	IF NOT EXISTS (
		SELECT 1 FROM information_schema.columns WHERE table_name = 'rag_chunks' AND column_name = 'has_numbers'
	) THEN
		ALTER TABLE rag_chunks ADD COLUMN has_numbers boolean generated always as (content ~ '\\d' or content ~ '%') stored;
	END IF;
	IF NOT EXISTS (
		SELECT 1 FROM pg_indexes WHERE indexname = 'idx_rag_chunks_hasnum'
	) THEN
		CREATE INDEX idx_rag_chunks_hasnum ON rag_chunks(has_numbers);
	END IF;
END$$;

-- Generalized product facts for non-deposit products (MVP)
CREATE TABLE IF NOT EXISTS product_facts (
  id uuid primary key default gen_random_uuid(),
  doc_id uuid,
  product_code text not null,
  channel text,
  currency text,
  fact_key text not null,
  term_days int,
  amount_min numeric,
  amount_max numeric,
  value_numeric numeric,
  value_text text,
  effective_from date,
  effective_to date,
  source_url text,
  created_at timestamptz not null default now()
);
CREATE INDEX IF NOT EXISTS idx_product_facts_keys ON product_facts(product_code, fact_key, currency, channel);
CREATE INDEX IF NOT EXISTS idx_product_facts_doc ON product_facts(doc_id);

-- Optional FKs (if rag_docs exists)
DO $$
BEGIN
	IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'rag_docs') THEN
		ALTER TABLE IF EXISTS product_rates DROP CONSTRAINT IF EXISTS product_rates_doc_fk;
		ALTER TABLE IF EXISTS product_rates ADD CONSTRAINT product_rates_doc_fk FOREIGN KEY (doc_id) REFERENCES rag_docs(id) ON DELETE SET NULL;
		ALTER TABLE IF EXISTS product_facts DROP CONSTRAINT IF EXISTS product_facts_doc_fk;
		ALTER TABLE IF EXISTS product_facts ADD CONSTRAINT product_facts_doc_fk FOREIGN KEY (doc_id) REFERENCES rag_docs(id) ON DELETE SET NULL;
	END IF;
END$$; 