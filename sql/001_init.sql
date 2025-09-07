-- extensions
create extension if not exists pg_trgm;
create extension if not exists unaccent;
create extension if not exists pgcrypto;

-- docs table (one row per playbook)
create table if not exists docs (
  id uuid primary key default gen_random_uuid(),
  product        text        not null unique,
  aliases        text[]      default '{}',
  version        date,
  source         text,
  storage_bucket text,
  storage_path   text,
  mime_type      text,
  file_size      bigint,
  sha256         text,
  lang           text        default 'ru',
  body           text        not null,
  created_at     timestamptz default now(),
  updated_at     timestamptz default now()
);

create or replace function touch_updated_at() returns trigger as $$
begin new.updated_at := now(); return new; end $$ language plpgsql;
drop trigger if exists trg_docs_touch on docs;
create trigger trg_docs_touch before update on docs
for each row execute function touch_updated_at();

-- passages
create table if not exists doc_passages (
  id bigserial primary key,
  doc_id   uuid references docs(id) on delete cascade,
  product  text not null,
  section  text,
  anchor   text,
  passage  text not null,
  ord      int  not null,
  tsv      tsvector
);

create index if not exists doc_passages_doc_id_idx on doc_passages(doc_id);
create index if not exists doc_passages_product_idx on doc_passages(product);
create index if not exists doc_passages_ord_idx on doc_passages(doc_id, ord);
create index if not exists doc_passages_tsv_idx on doc_passages using gin(tsv);
create index if not exists doc_passages_trgm_idx on doc_passages using gin (passage gin_trgm_ops);

-- tsvector builder
create or replace function doc_passages_tsv_update() returns trigger as $$
begin
  new.tsv :=
    setweight(to_tsvector('russian', coalesce(new.section,'')), 'A') ||
    setweight(to_tsvector('russian', unaccent(coalesce(new.anchor,''))), 'A') ||
    setweight(to_tsvector('russian', unaccent(new.passage)), 'B');
  return new;
end $$ language plpgsql;

drop trigger if exists tsv_update on doc_passages;
create trigger tsv_update
before insert or update on doc_passages
for each row execute function doc_passages_tsv_update();

-- RPC: import txt (splits on blank lines; tracks sections by '## ')
create or replace function import_doc_txt(
  p_product text,
  p_aliases text[],
  p_version date,
  p_source text,
  p_body text
) returns uuid
language plpgsql
as $$
declare
  v_doc_id uuid;
  v_block text;
  v_section text := null;
  ord_counter int := 0;
begin
  delete from docs where product = p_product;

  insert into docs(product, aliases, version, source, body)
  values (p_product, coalesce(p_aliases,'{}'), p_version, p_source, p_body)
  returning id into v_doc_id;

  for v_block in
    select trim(b) from regexp_split_to_table(p_body, E'\n\s*\n') as t(b)
  loop
    if v_block is null or length(v_block) < 2 then continue; end if;

    if v_block like '## %' then
      v_section := trim(substr(v_block, 4));
      continue;
    end if;

    ord_counter := ord_counter + 1;

    insert into doc_passages(doc_id, product, section, anchor, passage, ord)
    values (
      v_doc_id,
      p_product,
      v_section,
      (select regexp_match(v_block, '§[A-Za-zА-Яа-я0-9\-]+'))[1],
      v_block,
      ord_counter
    );
  end loop;

  return v_doc_id;
end $$;

-- RPC: search passages for a product
create or replace function search_passages(
  p_product text,
  p_query text,
  p_limit int default 8
) returns table (
  passage_id bigint,
  ord int,
  section text,
  anchor text,
  snippet text,
  rank numeric
)
language sql
stable
as $$
with q as (
  select
    plainto_tsquery('russian', p_query) as tsq,
    unaccent(p_query)                   as q_plain
)
select
  p.id,
  p.ord,
  p.section,
  p.anchor,
  ts_headline('russian', p.passage, q.tsq,
              'MaxFragments=2, MinWords=5, MaxWords=18, StartSel=**, StopSel=**') as snippet,
  (ts_rank_cd(p.tsv, q.tsq) * 0.8 + similarity(p.passage, q.q_plain) * 0.2) as rank
from doc_passages p, q
where p.product = p_product
  and (p.tsv @@ q.tsq or p.passage % q.q_plain)
order by rank desc, p.ord
limit p_limit;
$$;

-- RLS
alter table docs enable row level security;
alter table doc_passages enable row level security;

do $$ begin
  if not exists (select 1 from pg_policies where schemaname = 'public' and tablename = 'docs' and policyname = 'public read docs') then
    create policy "public read docs" on docs for select using (true);
  end if;
  if not exists (select 1 from pg_policies where schemaname = 'public' and tablename = 'doc_passages' and policyname = 'public read passages') then
    create policy "public read passages" on doc_passages for select using (true);
  end if;
end $$; 