import io
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from typing import Optional, List, Dict
from datetime import datetime

from .db import Database
from openai import OpenAI
from .config import get_settings


CHUNK_SIZE = 900
CHUNK_OVERLAP = 150

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
		# fallback: chunk by global size/overlap
		s = 0
		n = len(joined)
		while s < n:
			e = min(n, s + CHUNK_SIZE)
			sections.append(("Раздел", joined[s:e]))
			if e == n:
				break
			s = max(e - CHUNK_OVERLAP, s + 1)
		return sections
	# build sections between headings
	for i, (start, end, name) in enumerate(spans):
		next_start = spans[i + 1][0] if i + 1 < len(spans) else len(joined)
		body = joined[start:next_start].strip()
		if body:
			sections.append((name, body))
	return sections


def _pack_rule_sections_to_chunks(sections: list[tuple[str, str]]) -> list[str]:
	"""Pack sections to chunks preserving section headers using global CHUNK_SIZE/CHUNK_OVERLAP."""
	chunks: list[str] = []
	for name, body in sections:
		prefix = f"[{name}]\n"
		text = prefix + body
		if len(text) <= CHUNK_SIZE:
			chunks.append(text)
			continue
		# split large body
		start = 0
		while start < len(text):
			end = min(len(text), start + CHUNK_SIZE)
			chunks.append(text[start:end])
			if end == len(text):
				break
			start = max(end - CHUNK_OVERLAP, start + 1)
	return chunks


def ingest_kn_docs(db: Database) -> int:
	"""Ingest KN (КН) documents from rag_docs into rag_chunks. Returns number of docs processed."""
	try:
		docs = db.client.table("rag_docs").select("id,url,title,content,mime").eq("product_code", "КН").execute()
		rows: List[Dict] = getattr(docs, "data", []) or []
	except Exception:
		rows = []
	count = 0
	for doc in rows:
		try:
			doc_id = doc.get("id")
			url = (doc.get("url") or "").strip()
			title = doc.get("title") or url
			mime = doc.get("mime") or ""
			text = doc.get("content") or ""
			# Fetch content if missing
			if not text and url:
				try:
					mime, title, text = _fetch_text_from_url(url)
					db.client.table("rag_docs").update({"mime": mime, "title": title, "content": text}).eq("id", doc_id).execute()
				except Exception:
					continue
			if not text or not doc_id:
				continue
			# clear previous chunks for this doc
			try:
				db.client.table("rag_chunks").delete().eq("doc_id", doc_id).execute()
			except Exception:
				pass
			parts = _chunk_text(text)
			embeds = _embed_texts(parts)
			bulk = []
			for idx, part in enumerate(parts):
				if not part.strip():
					continue
				rowc = {
					"doc_id": doc_id,
					"product_code": "КН",
					"chunk_index": idx,
					"content": part,
				}
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


def ingest_deposit_docs(db: Database) -> int:
	"""Ingest Deposit (Вклад) documents from rag_docs into rag_chunks using rule-section chunking. Returns number of docs processed."""
	try:
		docs = db.client.table("rag_docs").select("id,url,title,content,mime").eq("product_code", "Вклад").execute()
		rows: List[Dict] = getattr(docs, "data", []) or []
	except Exception:
		rows = []
	count = 0
	for doc in rows:
		try:
			doc_id = doc.get("id")
			url = (doc.get("url") or "").strip()
			title = doc.get("title") or url
			mime = doc.get("mime") or ""
			text = doc.get("content") or ""
			if not text and url:
				try:
					mime, title, text = _fetch_text_from_url(url)
					db.client.table("rag_docs").update({"mime": mime, "title": title, "content": text}).eq("id", doc_id).execute()
				except Exception:
					continue
			if not text or not doc_id:
				continue
			# clear previous chunks for this doc
			try:
				db.client.table("rag_chunks").delete().eq("doc_id", doc_id).execute()
			except Exception:
				pass
			# extract rule sections and pack to chunks
			sections = _extract_deposit_rule_sections(text)
			parts = _pack_rule_sections_to_chunks(sections)
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