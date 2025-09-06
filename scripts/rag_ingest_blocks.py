#!/usr/bin/env python3
import os
import sys
import argparse
from typing import Optional, List, Tuple
from supabase import create_client, Client

STAGE_ALIASES = {
    "продажа": "продажа",
    "работа с возражениями": "возражения",
    "возражения": "возражения",
    "завершение контакта": "завершение",
}

GENERAL_PRODUCT = "Плейбуки"  # used for 'Общее'


def get_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_(SERVICE|ANON)_KEY in environment", file=sys.stderr)
        sys.exit(2)
    return create_client(url, key)


def download_text(client: Client, bucket: str, path: str) -> str:
    data = client.storage.from_(bucket).download(path)
    b = getattr(data, "data", data)
    if isinstance(b, bytes):
        for enc in ("utf-8", "cp1251", "latin1"):
            try:
                return b.decode(enc)
            except Exception:
                continue
        return b.decode("utf-8", errors="ignore")
    return str(b)


def parse_blocks(text: str) -> List[Tuple[str, str, str]]:
    """Return list of (stage, product_code, content). Stages marked by lines starting with '*', subblocks with '**'."""
    lines = [ln.rstrip() for ln in text.splitlines()]
    result: List[Tuple[str, str, str]] = []
    cur_stage: Optional[str] = None
    cur_product: Optional[str] = None
    buf: List[str] = []

    def flush():
        nonlocal buf, cur_stage, cur_product
        if cur_stage and cur_product and buf:
            content = "\n".join([ln for ln in buf]).strip()
            if content:
                result.append((cur_stage, cur_product, content))
        buf = []

    for raw in lines:
        ln = raw.strip()
        if not ln:
            if buf is not None:
                buf.append("")
            continue
        if ln.startswith("**"):
            # subblock (product)
            flush()
            name = ln.lstrip("* ")
            prod = name.strip()
            # normalize
            if prod.lower() == "общее":
                prod = GENERAL_PRODUCT
            cur_product = prod
            continue
        if ln.startswith("*"):
            # new stage
            flush()
            head = ln.lstrip("* ").lower()
            # remove inline parentheses
            if "(" in head:
                head = head.split("(", 1)[0].strip()
            norm = STAGE_ALIASES.get(head, head)
            cur_stage = norm
            cur_product = None
            continue
        # regular content
        if cur_stage and cur_product:
            buf.append(raw)
    flush()
    return result


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
    ap = argparse.ArgumentParser(description="Parse one TXT with * stages and ** product subblocks into rag_docs rows")
    ap.add_argument("--bucket", required=True, help="Storage bucket name")
    ap.add_argument("--path", required=True, help="Path to TXT inside bucket")
    ap.add_argument("--default_product", default=GENERAL_PRODUCT, help="Default product for 'Общее'")
    args = ap.parse_args()

    client = get_client()
    text = download_text(client, args.bucket, args.path)
    rows = parse_blocks(text)
    if not rows:
        print("No blocks parsed", file=sys.stderr)
        sys.exit(4)

    removed = purge_rag_docs(client)
    print(f"Purged rag_docs rows: {removed}")

    inserted = 0
    for stage, product, content in rows:
        title = f"{os.path.basename(args.path)} · {stage} · {product}"
        url = f"supabase://{args.bucket}/{args.path}#{stage}/{product}"
        row = {
            "url": url,
            "title": title,
            "product_code": product,
            "mime": "text/plain",
            "content": content,
        }
        # upsert by URL
        try:
            client.table("rag_docs").upsert(row, on_conflict="url").execute()
            inserted += 1
        except Exception:
            try:
                client.table("rag_docs").delete().eq("url", url).execute()
                client.table("rag_docs").insert(row).execute()
                inserted += 1
            except Exception:
                pass
    print(f"Inserted rows: {inserted}")

if __name__ == "__main__":
    main() 