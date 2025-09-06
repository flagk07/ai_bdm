#!/usr/bin/env python3
import os
import sys
import argparse
from typing import Optional
from supabase import create_client, Client
import re

try:
    from striprtf.striprtf import rtf_to_text as strip_rtf_to_text
except Exception:
    strip_rtf_to_text = None


def get_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_(SERVICE|ANON)_KEY in environment", file=sys.stderr)
        sys.exit(2)
    return create_client(url, key)


def list_first_doc(client: Client, bucket: str, prefix: Optional[str]) -> Optional[str]:
    storage = client.storage.from_(bucket)
    def _list(path: Optional[str]):
        try:
            return storage.list(path or "")
        except Exception:
            return []
    q = [prefix or ""]
    while q:
        cur = q.pop(0)
        items = _list(cur)
        for it in items:
            name = it.get("name")
            if not name:
                continue
            full = (cur + "/" + name) if cur else name
            # directory heuristic
            if it.get("id") is None and it.get("metadata") is None and it.get("created_at") is None:
                q.append(full)
                continue
            low = full.lower()
            if low.endswith(".txt") or low.endswith(".rtf"):
                return full
    return None


def decode_bytes(b: bytes) -> str:
    for enc in ("utf-8", "cp1251", "latin1"):
        try:
            return b.decode(enc)
        except Exception:
            continue
    return b.decode("utf-8", errors="ignore")


def naive_rtf_to_text(data: str) -> str:
    s = re.sub(r"{\\rtf[^}]*}", "", data, flags=re.IGNORECASE)
    s = re.sub(r"\\[a-zA-Z]+-?\d* ?", "", s)
    s = re.sub(r"[{}]", "", s)
    s = re.sub(r"\\'[0-9a-fA-F]{2}", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def download_text(client: Client, bucket: str, path: str) -> str:
    data = client.storage.from_(bucket).download(path)
    b = getattr(data, "data", data)
    raw = decode_bytes(b if isinstance(b, bytes) else str(b).encode("utf-8"))
    if path.lower().endswith(".rtf"):
        if strip_rtf_to_text is not None:
            try:
                return strip_rtf_to_text(raw)
            except Exception:
                return naive_rtf_to_text(raw)
        return naive_rtf_to_text(raw)
    return raw


def purge_rag_docs(client: Client) -> int:
    try:
        sel = client.table("rag_docs").select("id").execute()
        ids = [r["id"] for r in (getattr(sel, "data", []) or [])]
    except Exception:
        ids = []
    deleted = 0
    for rid in ids:
        try:
            client.table("rag_docs").delete().eq("id", rid).execute()
            deleted += 1
        except Exception:
            pass
    return deleted


def main() -> None:
    ap = argparse.ArgumentParser(description="Purge rag_docs and insert single TXT/RTF from Storage bucket")
    ap.add_argument("--bucket", required=True, help="Storage bucket name, e.g. Scripts")
    ap.add_argument("--prefix", default="", help="Optional path prefix/folder")
    ap.add_argument("--product_code", default="Плейбуки", help="Product code to store with the doc")
    args = ap.parse_args()

    client = get_client()
    path = list_first_doc(client, args.bucket, args.prefix or None)
    if not path:
        print("No TXT/RTF file found in bucket/prefix", file=sys.stderr)
        sys.exit(3)
    text = download_text(client, args.bucket, path)

    # Purge rag_docs robustly
    removed = purge_rag_docs(client)
    print(f"Purged rag_docs rows: {removed}")

    # Insert single row
    row = {
        "url": f"supabase://{args.bucket}/{path}",
        "title": os.path.basename(path),
        "product_code": args.product_code,
        "mime": "text/plain",
        "content": text,
    }
    client.table("rag_docs").insert(row).execute()
    print(f"Inserted 1 row from {args.bucket}/{path} as product_code={args.product_code}")


if __name__ == "__main__":
    main() 