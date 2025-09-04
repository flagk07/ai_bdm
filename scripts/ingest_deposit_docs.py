#!/usr/bin/env python3
import os
import argparse
import requests
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import List, Optional


def extract_pdf_text(data: bytes) -> str:
	from pypdf import PdfReader
	import io
	pdf = PdfReader(io.BytesIO(data))
	parts: List[str] = []
	for p in pdf.pages:
		try:
			parts.append(p.extract_text() or "")
		except Exception:
			pass
	return "\n".join(parts)


def upsert_doc(client: Client, url: str, title: str, product_code: str, mime: str, content: str) -> Optional[str]:
	row = {"url": url, "title": title, "product_code": product_code, "mime": mime, "content": content}
	try:
		ins = client.table("rag_docs").upsert(row, on_conflict="url").select("id").eq("url", url).maybe_single().execute()
		doc = getattr(ins, "data", None)
		if doc and doc.get("id"):
			return doc["id"]
	except Exception:
		try:
			client.table("rag_docs").delete().eq("url", url).execute()
			client.table("rag_docs").insert(row).execute()
			sel = client.table("rag_docs").select("id").eq("url", url).single().execute()
			return getattr(sel, "data", {}).get("id")
		except Exception:
			return None
	return None


def link_product_rates(client: Client, doc_url: str, doc_id: str) -> int:
	# Link depo_rates where source_url equals doc_url
	try:
		upd = client.table("depo_rates").update({"doc_id": doc_id}).eq("source_url", doc_url).execute()
		rows = getattr(upd, "data", None) or []
		return len(rows)
	except Exception:
		return 0


def main():
	load_dotenv()
	p = argparse.ArgumentParser(description="Ingest deposit docs into rag_docs and link depo_rates")
	p.add_argument("--urls", nargs='+', required=True, help="Document URLs (PDF)")
	p.add_argument("--product-code", default="Вклад")
	args = p.parse_args()

	url = os.getenv("SUPABASE_URL")
	key = os.getenv("SUPABASE_API_KEY") or os.getenv("SUPABASE_SERVICE_KEY")
	if not url or not key:
		raise SystemExit("Missing SUPABASE_URL or SUPABASE_API_KEY/SUPABASE_SERVICE_KEY env")
	client = create_client(url, key)

	total_linked = 0
	for doc_url in args.urls:
		print(f"[fetch] {doc_url}")
		resp = requests.get(doc_url, timeout=60)
		resp.raise_for_status()
		data = resp.content
		text = extract_pdf_text(data)
		title = os.path.basename(doc_url.split("?")[0])
		doc_id = upsert_doc(client, doc_url, title, args.product_code, "application/pdf", text)
		if not doc_id:
			print(f"[skip] cannot insert/select rag_docs for {doc_url}")
			continue
		linked = link_product_rates(client, doc_url, doc_id)
		total_linked += linked
		print(f"[ok] {doc_url} → doc_id={doc_id}, linked depo_rates={linked}")

	print(f"Done. Linked depo_rates: {total_linked}")


if __name__ == "__main__":
	main() 