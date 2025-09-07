-- =========================================
-- Minimal public schema for AI BDM (8 tables)
-- =========================================
SET search_path = public;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- employees
CREATE TABLE IF NOT EXISTS employees (
  tg_id       bigint PRIMARY KEY,
  agent_name  text,
  active      boolean NOT NULL DEFAULT true,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- allowed_users
CREATE TABLE IF NOT EXISTS allowed_users (
  tg_id      bigint PRIMARY KEY,
  active     boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- meet
CREATE TABLE IF NOT EXISTS meet (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tg_id        bigint NOT NULL REFERENCES employees(tg_id) ON DELETE RESTRICT,
  product_code text   NOT NULL,
  for_date     date   NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_meet_tg_date_prod ON meet(tg_id, for_date, product_code);
CREATE INDEX IF NOT EXISTS idx_meet_created_at ON meet(created_at DESC);

-- attempts
CREATE TABLE IF NOT EXISTS attempts (
  id            bigserial PRIMARY KEY,
  tg_id         bigint NOT NULL,
  product_code  text   NOT NULL,
  attempt_count integer NOT NULL CHECK (attempt_count >= 0),
  for_date      date   NOT NULL,
  meet_id       uuid   NULL REFERENCES meet(id) ON DELETE SET NULL,
  created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_attempts_tg_date ON attempts(tg_id, for_date);
CREATE INDEX IF NOT EXISTS idx_attempts_tg_date_prod ON attempts(tg_id, for_date, product_code);

-- sales_plans
CREATE TABLE IF NOT EXISTS sales_plans (
  tg_id      bigint NOT NULL,
  year       integer NOT NULL,
  month      integer NOT NULL CHECK (month BETWEEN 1 AND 12),
  plan_month integer NOT NULL CHECK (plan_month >= 0),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tg_id, year, month)
);

-- assistant_messages
CREATE TABLE IF NOT EXISTS assistant_messages (
  id                 bigserial PRIMARY KEY,
  tg_id              bigint NOT NULL,
  role               text   NOT NULL,
  content_sanitized  text   NOT NULL,
  off_topic          boolean NOT NULL DEFAULT false,
  auto               boolean NOT NULL DEFAULT false,
  created_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_am_tg_created ON assistant_messages(tg_id, created_at DESC);

-- notes
CREATE TABLE IF NOT EXISTS notes (
  id                bigserial PRIMARY KEY,
  tg_id             bigint NOT NULL,
  content_sanitized text   NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_notes_tg_created ON notes(tg_id, created_at DESC);

-- logs
CREATE TABLE IF NOT EXISTS logs (
  id         bigserial PRIMARY KEY,
  tg_id      bigint NULL,
  action     text   NOT NULL,
  payload    jsonb  NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_logs_created ON logs(created_at DESC);

-- RLS (permissive for server-side bot)
ALTER TABLE employees ENABLE ROW LEVEL SECURITY;
ALTER TABLE allowed_users ENABLE ROW LEVEL SECURITY;
ALTER TABLE meet ENABLE ROW LEVEL SECURITY;
ALTER TABLE attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE sales_plans ENABLE ROW LEVEL SECURITY;
ALTER TABLE assistant_messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE notes ENABLE ROW LEVEL SECURITY;
ALTER TABLE logs ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
  EXECUTE 'DROP POLICY IF EXISTS anon_all_employees ON employees';
  EXECUTE 'CREATE POLICY anon_all_employees ON employees FOR ALL USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS anon_all_allowed_users ON allowed_users';
  EXECUTE 'CREATE POLICY anon_all_allowed_users ON allowed_users FOR ALL USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS anon_all_meet ON meet';
  EXECUTE 'CREATE POLICY anon_all_meet ON meet FOR ALL USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS anon_all_attempts ON attempts';
  EXECUTE 'CREATE POLICY anon_all_attempts ON attempts FOR ALL USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS anon_all_sales_plans ON sales_plans';
  EXECUTE 'CREATE POLICY anon_all_sales_plans ON sales_plans FOR ALL USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS anon_all_assistant_messages ON assistant_messages';
  EXECUTE 'CREATE POLICY anon_all_assistant_messages ON assistant_messages FOR ALL USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS anon_all_notes ON notes';
  EXECUTE 'CREATE POLICY anon_all_notes ON notes FOR ALL USING (true) WITH CHECK (true)';
  EXECUTE 'DROP POLICY IF EXISTS anon_all_logs ON logs';
  EXECUTE 'CREATE POLICY anon_all_logs ON logs FOR ALL USING (true) WITH CHECK (true)';
END $$; 