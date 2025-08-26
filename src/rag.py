import io
import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from typing import Optional
from datetime import datetime

from .db import Database


def _fetch_text_from_url(url: str) -> tuple[str, str, str]:
	"""Return (mime, title, text). Supports HTML and PDF."""
	r = requests.get(url, timeout=20)
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
		return ("application/pdf", url.split("/")[-1], "\n".join(texts))
	# assume HTML
	html = r.text
	soup = BeautifulSoup(html, "html.parser")
	title = soup.title.text.strip() if soup.title else url
	for s in soup(["script", "style", "noscript"]):
		s.extract()
	text = " ".join(soup.get_text(" ").split())
	return ("text/html", title, text)


def ingest_kn_docs(db: Database) -> int:
	"""Ingest KN (КН) documents into rag_docs. Returns count stored."""
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
			# upsert by url
			db.client.table("rag_docs").upsert({
				"url": u,
				"title": title,
				"product_code": "КН",
				"mime": mime,
				"content": text,
			}, on_conflict="url").execute()
			count += 1
		except Exception:
			continue
	return count 