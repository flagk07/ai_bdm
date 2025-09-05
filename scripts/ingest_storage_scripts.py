#!/usr/bin/env python3
import os
import sys
import argparse
from typing import List, Dict, Tuple, Optional, Set

from supabase import create_client, Client

PRODUCT_PATTERNS: List[Tuple[str, str]] = [
    ("Вклад", r"вклад|депозит"),
    ("КН", r"кн|кредит налич|потреб"),
    ("КК", r"кк|кредитн[а-я\s]*карт"),
    ("ДК", r"дк|дебетова[я|я\s]карт|дебетов[а-я\s]*карт"),
    ("ИК", r"ипотек"),
    ("НС", r"накопит(ельн|) счет|накопит"),
    ("ИЗП", r"зарплатн|зарплатный проект|изп"),
    ("КН к ЗП", r"кн к зп|кредит к зарплат"),
]


def get_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_(SERVICE|ANON)_KEY in environment", file=sys.stderr)
        sys.exit(2)
    return create_client(url, key)


def list_txt_paths(client: Client, bucket: str, prefix: Optional[str]) -> List[str]:
    storage = client.storage.from_(bucket)
    # Recursively list
    def _list(path: Optional[str]) -> List[str]:
        items = storage.list(path or "")
        files: List[str] = []
        for it in items:
            name = it.get("name")
            if not name:
                continue
            # subfolder
            if it.get("id") is None and it.get("metadata") is None and it.get("created_at") is None:
                # This is likely a folder marker in some SDKs; try recursing
                sub = (path + "/" + name) if path else name
                files.extend(_list(sub))
            else:
                full = (path + "/" + name) if path else name
                if full.lower().endswith(".txt"):
                    files.append(full)
        return files
    return _list(prefix)


def download_text(client: Client, bucket: str, path: str) -> str:
    data = client.storage.from_(bucket).download(path)
    if isinstance(data, bytes):
        try:
            return data.decode("utf-8")
        except Exception:
            return data.decode("cp1251", errors="ignore")
    # supabase-py may return dict with data
    b = getattr(data, "data", None)
    if isinstance(b, bytes):
        try:
            return b.decode("utf-8")
        except Exception:
            return b.decode("cp1251", errors="ignore")
    return str(data)


def detect_products(text: str) -> Set[str]:
    import re
    low = text.lower()
    found: Set[str] = set()
    for code, pat in PRODUCT_PATTERNS:
        if re.search(pat, low):
            found.add(code)
    if not found:
        # Fallback to generic coaching bucket
        found.add("Плейбуки")
    return found


def upsert_rag_docs(client: Client, bucket: str, path: str, text: str, product_codes: Set[str]) -> int:
    count = 0
    title = os.path.basename(path)
    for code in product_codes:
        row = {
            "url": f"supabase://{bucket}/{path}#{code}",
            "title": f"{title} · {code}",
            "product_code": code,
            "mime": "text/plain",
            "content": text,
        }
        try:
            client.table("rag_docs").upsert(row, on_conflict="url").execute()
            count += 1
        except Exception as e:
            print(f"[warn] upsert failed for {path} ({code}): {e}")
    return count


def public_or_signed_url(client: Client, bucket: str, path: str, expires: int = 7 * 24 * 3600) -> str:
    try:
        res = client.storage.from_(bucket).get_public_url(path)
        url = res.get("publicUrl") or res.get("public_url") or ""
        if url:
            return url
    except Exception:
        pass
    try:
        res = client.storage.from_(bucket).create_signed_url(path, expires)
        url = res.get("signedURL") or res.get("signedUrl") or res.get("signed_url") or ""
        if url:
            return url
    except Exception:
        pass
    return f"supabase://{bucket}/{path}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest TXT scripts from Supabase Storage bucket into rag_docs and print URLs")
    parser.add_argument("--bucket", default="Scripts", help="Supabase Storage bucket name")
    parser.add_argument("--prefix", default="", help="Optional prefix (folder) inside bucket")
    parser.add_argument("--print_urls", action="store_true", help="Only print URLs and exit")
    parser.add_argument("--ingest", action="store_true", help="Ingest into rag_docs")
    args = parser.parse_args()

    client = get_client()
    paths = list_txt_paths(client, args.bucket, args.prefix or None)
    if not paths:
        print("No TXT files found")
        return

    urls: List[str] = [public_or_signed_url(client, args.bucket, p) for p in paths]
    if args.print_urls and not args.ingest:
        for u in urls:
            print(u)
        print(f"Total: {len(urls)}")
        return

    inserted = 0
    for p in paths:
        text = download_text(client, args.bucket, p)
        products = detect_products(text)
        inserted += upsert_rag_docs(client, args.bucket, p, text, products)
        print(f"[ok] {p}: products={sorted(products)}")
    print(f"Done. Upserted rag_docs rows: {inserted} from {len(paths)} files")


if __name__ == "__main__":
    main() 