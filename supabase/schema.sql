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
  product_code text not null check (product_code in ('КН','КСП','ПУ','ДК','ИК','ИЗП','НС','Вклад')),
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

-- Logs
create table if not exists logs (
  id uuid primary key default gen_random_uuid(),
  tg_id bigint,
  action text not null,
  payload jsonb,
  created_at timestamptz not null default now()
);
create index if not exists idx_logs_created on logs (created_at desc);

-- RLS Policies (MVP: allow anon role to read/write). Adjust for production.
alter table allowed_users enable row level security;
alter table employees enable row level security;
alter table attempts enable row level security;
alter table notes enable row level security;
alter table assistant_messages enable row level security;
alter table logs enable row level security;

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