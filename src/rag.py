import io
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from typing import Optional
from datetime import datetime

from .db import Database


CHUNK_SIZE = 900
CHUNK_OVERLAP = 150


def _fetch_text_from_url(url: str) -> tuple[str, str, str]:
	"""Return (mime, title, text). Supports HTML and PDF."""
	h = {"User-Agent": "ai-bdm-rag/1.0"}
	r = requests.get(url, timeout=20, headers=h)
	r.raise_for_status()
	ct = r.headers.get("Content-Type", "").lower()
	if "pdf" in ct or url.lower().endswith(".pdf"):
		pdf = PdfReader(io.BytesIO(r.content))
		texts = []
		for page in pdf.pages:
			try:
				texts.append(page.extract_text() or "")
			except Exception:
				pass
		return ("application/pdf", url.split("/")[-1], "\n".join(texts).strip())
	# assume HTML
	html = r.text
	soup = BeautifulSoup(html, "html.parser")
	title = soup.title.text.strip() if soup.title else url
	for s in soup(["script", "style", "noscript"]):
		s.extract()
	text = " ".join(soup.get_text(" ").split()).strip()
	return ("text/html", title, text)


def _chunk_text(text: str) -> list[str]:
	chunks: list[str] = []
	if not text:
		return chunks
	start = 0
	n = len(text)
	while start < n:
		end = min(n, start + CHUNK_SIZE)
		chunks.append(text[start:end])
		if end == n:
			break
		start = max(end - CHUNK_OVERLAP, start + 1)
	return chunks


def ingest_kn_docs(db: Database) -> int:
	"""Ingest KN (КН) documents into rag_docs and rag_chunks. Returns count stored."""
	urls = [
		"https://domrfbank.ru/loans/credit/?from=menu&type=link&product=credit",
		"https://domrfbank.ru/upload/medialibrary/004/%D0%9E%D0%B1%D1%89%D0%B8%D0%B5%20%D1%83%D1%81%D0%BB%D0%BE%D0%B2%D0%B8%D1%8F%20%D0%BF%D1%80%D0%B5%D0%B4%D0%BE%D1%81%D1%82%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%BA%D1%80%D0%B5%D0%B4%D0%B8%D1%82%D0%BE%D0%B2%20%D1%84%D0%B8%D0%B7.%20%D0%BB%D0%B8%D1%86%D0%B0%D0%BC_%D1%81%2014.02.2025.pdf",
		"https://domrfbank.ru/upload/docs/loans/%D0%98%D0%BD%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%86%D0%B8%D1%8F%20%D0%BE%D0%B1%20%D1%83%D1%81%D0%BB%D0%BE%D0%B2%D0%B8%D1%8F%D1%85%20%D0%BF%D1%80%D0%B5%D0%B4%D0%BE%D1%81%D1%82%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F%20%D0%BA%D1%80%D0%B5%D0%B4%D0%B8%D1%82%D0%B0.pdf",
		"https://domrfbank.ru/upload/docs/loans/Tarify_PK_Kredit_nalichnymi.pdf",
		"https://domrfbank.ru/upload/docs/loans/Informaciya_o_PSK.pdf",
	]
	count = 0
	for u in urls:
		try:
			mime, title, text = _fetch_text_from_url(u)
			if not text:
				continue
			row = {
				"url": u,
				"title": title,
				"product_code": "КН",
				"mime": mime,
				"content": text,
			}
			# upsert doc
			doc_id = None
			try:
				ins = db.client.table("rag_docs").upsert(row, on_conflict="url").select("id").eq("url", u).maybe_single().execute()
				doc = getattr(ins, "data", None)
				if doc and doc.get("id"):
					doc_id = doc["id"]
			except Exception:
				# fallback: delete+insert then select
				try:
					db.client.table("rag_docs").delete().eq("url", u).execute()
					db.client.table("rag_docs").insert(row).execute()
					sel = db.client.table("rag_docs").select("id").eq("url", u).single().execute()
					doc_id = getattr(sel, "data", {}).get("id")
				except Exception:
					continue
			if not doc_id:
				# try select existing
				try:
					sel2 = db.client.table("rag_docs").select("id").eq("url", u).single().execute()
					doc_id = getattr(sel2, "data", {}).get("id")
				except Exception:
					pass
			if not doc_id:
				continue
			# write chunks: clear previous chunks for this doc
			try:
				db.client.table("rag_chunks").delete().eq("doc_id", doc_id).execute()
			except Exception:
				pass
			parts = _chunk_text(text)
			bulk = []
			for idx, part in enumerate(parts):
				if not part.strip():
					continue
				bulk.append({
					"doc_id": doc_id,
					"product_code": "КН",
					"chunk_index": idx,
					"content": part,
				})
			if bulk:
				db.client.table("rag_chunks").insert(bulk).execute()
			count += 1
		except Exception:
			continue
	return count


def ingest_deposit_docs(db: Database) -> int:
	"""Ingest Deposit (Вклад) documents into rag_docs and rag_chunks. Returns count stored."""
	urls = [
		"https://domrfbank.ru/deposits/?from=menu&type=link&product=deposit",
		"https://domrfbank.ru/upload/iblock/e30/8f5e7uepc4qpl17ane1649cwtzdohw09/%D0%A3%D1%81%D0%BB%D0%BE%D0%B2%D0%B8%D1%8F%20%D0%BE%D1%82%D0%BA%D1%80%D1%8B%D1%82%D0%B8%D1%8F%20%D1%81%D1%80%D0%BE%D1%87%D0%BD%D0%BE%D0%B3%D0%BE%20%D0%B1%D0%B0%D0%BD%D0%BA%D0%BE%D0%B2%D1%81%D0%BA%D0%BE%D0%B3%D0%BE%20%D0%B2%D0%BA%D0%BB%D0%B0%D0%B4%D0%B0%20%D0%B2%20%D1%81%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B5%20%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82-%D0%91%D0%B0%D0%BD%D0%BA.pdf",
		"https://domrfbank.ru/upload/iblock/742/wm0xwdyehlgina22903odn25dd4ef67o/%D0%A4%D0%B8%D0%BD%D0%B0%D0%BD%D1%81%D0%BE%D0%B2%D1%8B%D0%B5%20%D1%83%D1%81%D0%BB%D0%BE%D0%B2%D0%B8%D1%8F%20%D0%9C%D0%BE%D0%B9%20%D0%94%D0%BE%D0%BC%20%D0%BF%D1%80%D0%B8%20%D0%BE%D1%82%D0%BA%D1%80%D1%8B%D1%82%D0%B8%D0%B8%20%D0%B2%20%D1%81%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B5%20%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82-%D0%91%D0%B0%D0%BD%D0%BA.pdf",
		"https://domrfbank.ru/upload/iblock/e14/iiy6cpse137wcgc7n7lo9jfq1pdj5y57/%D0%9F%D1%80%D0%B0%D0%B2%D0%B8%D0%BB%D0%B0%20%D1%80%D0%B0%D0%B7%D0%BC%D0%B5%D1%89%D0%B5%D0%BD%D0%B8%D1%8F%20%D1%81%D1%80%D0%BE%D1%87%D0%BD%D1%8B%D1%85%20%D0%B1%D0%B0%D0%BD%D0%BA%D0%BE%D0%B2%D1%81%D0%BA%D0%B8%D1%85%20%D0%B2%D0%BA%D0%BB%D0%B0%D0%B4%D0%BE%D0%B2%20%D1%84%D0%B8%D0%B7.%20%D0%BB%D0%B8%D1%86%20%D0%BF%D1%80%D0%B8%20%D0%BE%D0%B1%D1%80%D0%B0%D1%89%D0%B5%D0%BD%D0%B8%D0%B8%20%D0%B2%20%D0%BE%D1%84%D0%B8%D1%81%20%D1%81%2019.12.2022.pdf",
		"https://domrfbank.ru/upload/iblock/d57/avsd2xiu531yvsjuq9m6aq41b713dh0u/%D0%A4%D0%B8%D0%BD%D0%B0%D0%BD%D1%81%D0%BE%D0%B2%D1%8B%D0%B5%20%D1%83%D1%81%D0%BB%D0%BE%D0%B2%D0%B8%D1%8F%20%D0%BF%D0%BE%20%D0%B2%D0%BA%D0%BB%D0%B0%D0%B4%D0%B0%D0%BC%20%D1%84%D0%B8%D0%B7%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8%D1%85%20%D0%BB%D0%B8%D1%86%20%D1%81%2023.08.2025.pdf",
	]
	count = 0
	for u in urls:
		try:
			mime, title, text = _fetch_text_from_url(u)
			if not text:
				continue
			row = {
				"url": u,
				"title": title,
				"product_code": "Вклад",
				"mime": mime,
				"content": text,
			}
			# upsert doc
			doc_id = None
			try:
				ins = db.client.table("rag_docs").upsert(row, on_conflict="url").select("id").eq("url", u).maybe_single().execute()
				doc = getattr(ins, "data", None)
				if doc and doc.get("id"):
					doc_id = doc["id"]
			except Exception:
				# fallback: delete+insert then select
				try:
					db.client.table("rag_docs").delete().eq("url", u).execute()
					db.client.table("rag_docs").insert(row).execute()
					sel = db.client.table("rag_docs").select("id").eq("url", u).single().execute()
					doc_id = getattr(sel, "data", {}).get("id")
				except Exception:
					continue
			if not doc_id:
				# try select existing
				try:
					sel2 = db.client.table("rag_docs").select("id").eq("url", u).single().execute()
					doc_id = getattr(sel2, "data", {}).get("id")
				except Exception:
					pass
			if not doc_id:
				continue
			# write chunks: clear previous chunks for this doc
			try:
				db.client.table("rag_chunks").delete().eq("doc_id", doc_id).execute()
			except Exception:
				pass
			parts = _chunk_text(text)
			bulk = []
			for idx, part in enumerate(parts):
				if not part.strip():
					continue
				bulk.append({
					"doc_id": doc_id,
					"product_code": "Вклад",
					"chunk_index": idx,
					"content": part,
				})
			if bulk:
				db.client.table("rag_chunks").insert(bulk).execute()
			count += 1
		except Exception:
			continue
	return count 