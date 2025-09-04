#!/usr/bin/env python3
import os
import argparse
import requests
from typing import List, Optional
from supabase import create_client, Client
from dotenv import load_dotenv

# PDF text extraction
import io
try:
	import pdfplumber  # type: ignore
except Exception:
	pdfplumber = None


def extract_pdf_text(data: bytes) -> str:
	if pdfplumber is None:
		# Fallback: best effort decode
		return data.decode("utf-8", errors="ignore")
	text_parts: List[str] = []
	with pdfplumber.open(io.BytesIO(data)) as pdf:
		for page in pdf.pages:
			try:
				t = page.extract_text() or ""
			except Exception:
				t = ""
			text_parts.append(t)
	return "\n".join(text_parts).strip()


def chunk_text(text: str, chunk_size: int = 750, overlap: int = 150) -> List[str]:
	text = text or ""
	if not text:
		return []
	chunks: List[str] = []
	start = 0
	n = len(text)
	while start < n:
		end = min(n, start + chunk_size)
		chunk = text[start:end]
		chunks.append(chunk)
		if end >= n:
			break
		start = end - overlap
		if start < 0:
			start = 0
	return chunks


def upsert_doc(client: Client, url: str, title: str, product_code: str, mime: str, content: str) -> Optional[str]:
	# Try existing
	try:
		res = client.table("rag_docs").select("id").eq("url", url).maybe_single().execute()
		row = getattr(res, "data", None)
		if row and row.get("id"):
			return row["id"]
	except Exception:
		pass
	# Insert
	payload = {"url": url, "title": title, "product_code": product_code, "mime": mime, "content": content[:1_000_000]}
	ins = client.table("rag_docs").insert(payload).execute()
	rows = getattr(ins, "data", None) or []
	return rows[0]["id"] if rows else None


def replace_chunks(client: Client, doc_id: str, product_code: str, chunks: List[str]) -> None:
	# Delete previous chunks for this doc
	try:
		client.table("rag_chunks").delete().eq("doc_id", doc_id).execute()
	except Exception:
		pass
	if not chunks:
		return
	rows = []
	for idx, ch in enumerate(chunks):
		rows.append({
			"doc_id": doc_id,
			"product_code": product_code,
			"chunk_index": idx,
			"content": ch,
		})
	# Insert in batches to avoid payload limits
	batch = 500
	for i in range(0, len(rows), batch):
		client.table("rag_chunks").insert(rows[i:i+batch]).execute()


def link_product_rates(client: Client, doc_url: str, doc_id: str) -> int:
	# Link product_rates where source_url equals doc_url
	try:
		upd = client.table("product_rates").update({"doc_id": doc_id}).eq("source_url", doc_url).execute()
		rows = getattr(upd, "data", None) or []
		return len(rows)
	except Exception:
		return 0


def main():
	load_dotenv()
	p = argparse.ArgumentParser(description="Ingest deposit docs into rag_docs and rag_chunks, link product_rates")
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
		chunks = chunk_text(text)
		replace_chunks(client, doc_id, args.product_code, chunks)
		linked = link_product_rates(client, doc_url, doc_id)
		total_linked += linked
		print(f"[ok] {doc_url} → doc_id={doc_id}, chunks={len(chunks)}, linked product_rates={linked}")

	print(f"Done. Linked product_rates: {total_linked}")

if __name__ == "__main__":
	main() 