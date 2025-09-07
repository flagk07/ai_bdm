#!/usr/bin/env python3
import os
import sys
from datetime import date
from supabase import create_client, Client

BUCKET = os.environ.get("STORAGE_BUCKET", "Scripts")
PATH = os.environ.get("STORAGE_PATH", "Playbook_formatted.txt")
PRODUCT = os.environ.get("PLAYBOOK_PRODUCT", "Плейбук")
ALIASES = ["playbook","скрипты","возражения"]


def get_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_(SERVICE|ANON)_KEY in environment", file=sys.stderr)
        sys.exit(2)
    return create_client(url, key)


def main() -> None:
    client = get_client()
    # download text from storage
    dl = client.storage.from_(BUCKET).download(PATH)
    if getattr(dl, "error", None):
        raise RuntimeError(getattr(dl, "error"))
    raw = getattr(dl, "data", dl)
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="ignore")
    else:
        # supabase-py may wrap
        data = getattr(raw, "data", raw)
        text = data.decode("utf-8", errors="ignore") if isinstance(data, bytes) else str(data)
    # call import_doc_txt RPC
    version = date.today().isoformat()
    source = f"{BUCKET}/{PATH}"
    res = client.rpc("import_doc_txt", {
        "p_product": PRODUCT,
        "p_aliases": ALIASES,
        "p_version": version,
        "p_source": source,
        "p_body": text,
    }).execute()
    doc_id = getattr(res, "data", None)
    if getattr(res, "error", None):
        raise RuntimeError(getattr(res, "error"))
    print(f"✅ Imported doc_id: {doc_id}")

    # quick probe: search a sample query
    q = os.environ.get("PROBE_QUERY", "как отрабатывать возражение дорого")
    srch = client.rpc("search_passages", {
        "p_product": PRODUCT,
        "p_query": q,
        "p_limit": 5,
    }).execute()
    rows = getattr(srch, "data", []) or []
    print(f"🔎 search '{q}' → {len(rows)} hits")
    for r in rows:
        print(f"- §{r['ord']} [{r.get('section') or ''}] {str(r.get('snippet') or '')[:160]}")


if __name__ == "__main__":
    main() 