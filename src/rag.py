import io
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from typing import Optional
from datetime import datetime

from .db import Database
from openai import OpenAI
from .config import get_settings


CHUNK_SIZE = 900
CHUNK_OVERLAP = 150

# Approx chars per token ≈ 4
DEPOSIT_RULE_CHARS = 3200  # ~800 tokens
DEPOSIT_RULE_OVERLAP = 480  # ~120 tokens

CURRENCIES = {
	"RUB": ["руб", "₽", "rub", "в руб", "руб.", "% год"],
	"USD": ["usd", "$", "доллар"],
	"EUR": ["eur", "€", "евро"],
	"CNY": ["cny", "¥", "юан" , "юани"],
}


def _infer_currency(text: str) -> Optional[str]:
	low = text.lower()
	for code, keys in CURRENCIES.items():
		for k in keys:
			if k in low:
				return code
	return None


def _embed_texts(texts: list[str]) -> list[Optional[list[float]]]:
	"""Return list of embeddings (1536 dim) using OpenAI; None if failed."""
	if not texts:
		return []
	try:
		client = OpenAI(api_key=get_settings().openai_api_key)
		resp = client.embeddings.create(model="text-embedding-3-small", input=texts)
		return [item.embedding if getattr(item, "embedding", None) else None for item in resp.data]
	except Exception:
		return [None for _ in texts]


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


def _is_probably_rate_table_line(line: str) -> bool:
	"""Heuristics to drop lines that look like rate tables (to keep numbers out of RAG)."""
	l = line.strip()
	if not l:
		return False
	import re as _re
	# contains % and many digits or separators
	if _re.search(r"\d{1,2}[.,]\d\s*%", l):
		return True
	# many semicolons or pipes typical for table exports
	if l.count("|") >= 2 or l.count(";") >= 3:
		return True
	# looks like a header row with many numbers
	if len(_re.findall(r"\d", l)) >= 6 and any(k in l.lower() for k in ["дн", "руб", "%"]):
		return True
	return False


def _extract_deposit_rule_sections(raw_text: str) -> list[tuple[str, str]]:
	"""Return list of (section_name, content) for deposit rules, excluding numeric rate tables.
	Looks for headings related to: пополнение, частичное снятие, оплата процентов/капитализация,
	лимиты по сумме и допвзносам, общие ограничения. Falls back to splitting by paragraphs.
	"""
	import re as _re
	text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
	lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
	# filter out likely table lines
	lines = [ln for ln in lines if not _is_probably_rate_table_line(ln)]
	joined = "\n".join(lines)
	# define patterns for sections
	patterns = [
		(r"(?i)пополнени[ея][^\n]*", "Пополнение вклада"),
		(r"(?i)частичн[ао]\s+снят[ие][^\n]*", "Частичное снятие"),
		(r"(?i)(оплат[аи]\s+процент|выплат[аи]\s+процент|капитализаци)[^\n]*", "Оплата процентов / капитализация"),
		(r"(?i)(максимальн[аяые]\s+сумм|не\s+более\s+15\s*\d{3}\s*\d{3}|3\s*[×x]\s*первоначального)[^\n]*", "Максимальная сумма и лимиты"),
		(r"(?i)(минимальн[аяые]\s+сумма\s+допвзнос|минимальн[аяые]\s+сумма\s+взнос)[^\n]*", "Минимальная сумма допвзноса"),
		(r'(?i)(ограничени[яй]|услови[яй]\s+размещени[яй]|правил[а])[^"\n]*', "Общие условия/ограничения"),
	]
	# find indices of headings
	spans: list[tuple[int, int, str]] = []
	for pat, name in patterns:
		for m in _re.finditer(pat, joined):
			spans.append((m.start(), m.end(), name))
	spans.sort(key=lambda x: x[0])
	sections: list[tuple[str, str]] = []
	if not spans:
		# fallback: chunk by size with deposit rule sizing
		s = 0
		n = len(joined)
		while s < n:
			e = min(n, s + DEPOSIT_RULE_CHARS)
			sections.append(("Раздел", joined[s:e]))
			if e == n:
				break
			s = max(e - DEPOSIT_RULE_OVERLAP, s + 1)
		return sections
	# build sections between headings
	for i, (start, end, name) in enumerate(spans):
		next_start = spans[i + 1][0] if i + 1 < len(spans) else len(joined)
		body = joined[start:next_start].strip()
		if body:
			sections.append((name, body))
	return sections


def _pack_rule_sections_to_chunks(sections: list[tuple[str, str]]) -> list[str]:
	"""Pack sections to chunks of ~800 tokens with overlap ~120 tokens preserving section headers."""
	chunks: list[str] = []
	for name, body in sections:
		prefix = f"[{name}]\n"
		text = prefix + body
		if len(text) <= DEPOSIT_RULE_CHARS:
			chunks.append(text)
			continue
		# split large body
		start = 0
		while start < len(text):
			end = min(len(text), start + DEPOSIT_RULE_CHARS)
			chunks.append(text[start:end])
			if end == len(text):
				break
			start = max(end - DEPOSIT_RULE_OVERLAP, start + 1)
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
			# prepare embeddings in batch
			embeds = _embed_texts(parts)
			bulk = []
			for idx, part in enumerate(parts):
				if not part.strip():
					continue
				cur = _infer_currency(part)  # may be None
				rowc = {
					"doc_id": doc_id,
					"product_code": "КН",
					"chunk_index": idx,
					"content": part,
				}
				if cur:
					rowc["currency"] = cur
				emb = embeds[idx] if idx < len(embeds) else None
				if emb:
					rowc["embedding"] = emb
				bulk.append(rowc)
			if bulk:
				db.client.table("rag_chunks").insert(bulk).execute()
			count += 1
		except Exception:
			continue
	return count


def ingest_deposit_docs(db: Database) -> int:
	"""Ingest Deposit (Вклад) documents into rag_docs and rag_chunks with rule-based chunking (no rate tables). Returns count stored."""
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
			# clear previous chunks for this doc
			try:
				db.client.table("rag_chunks").delete().eq("doc_id", doc_id).execute()
			except Exception:
				pass
			# extract rule sections and pack to chunks
			sections = _extract_deposit_rule_sections(text)
			parts = _pack_rule_sections_to_chunks(sections)
			# embed
			embeds = _embed_texts(parts)
			bulk = []
			for idx, part in enumerate(parts):
				if not part.strip():
					continue
				rowc = {
					"doc_id": doc_id,
					"product_code": "Вклад",
					"chunk_index": idx,
					"content": part,
				}
				# currency not enforced for rules; infer only if clearly indicated
				cur = _infer_currency(part)
				if cur:
					rowc["currency"] = cur
				emb = embeds[idx] if idx < len(embeds) else None
				if emb:
					rowc["embedding"] = emb
				bulk.append(rowc)
			if bulk:
				db.client.table("rag_chunks").insert(bulk).execute()
			count += 1
		except Exception:
			continue
	return count 