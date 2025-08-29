#!/usr/bin/env python3
import os
import re
import json
import argparse
import tempfile
import datetime as dt
import time
import requests
import pdfplumber
from typing import Optional, Tuple, List, Dict

DEPOSIT_URLS = [
	"https://domrfbank.ru/upload/iblock/742/wm0xwdyehlgina22903odn25dd4ef67o/%D0%A4%D0%B8%D0%BD%D0%B0%D0%BD%D1%81%D0%BE%D0%B2%D1%8B%D0%B5%20%D1%83%D1%81%D0%BB%D0%BE%D0%B2%D0%B8%D1%8F%20%D0%9C%D0%BE%D0%B9%20%D0%94%D0%BE%D0%BC%20%D0%BF%D1%80%D0%B8%20%D0%BE%D1%82%D0%BA%D1%80%D1%8B%D1%82%D0%B8%D0%B8%20%D0%B2%20%D1%81%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B5%20%D0%98%D0%BD%D1%82%D0%B5%D1%80%D0%BD%D0%B5%D1%82-%D0%91%D0%B0%D0%BD%D0%BA.pdf",
	"https://domrfbank.ru/upload/iblock/d57/avsd2xiu531yvsjuq9m6aq41b713dh0u/%D0%A4%D0%B8%D0%BD%D0%B0%D0%BD%D1%81%D0%BE%D0%B2%D1%8B%D0%B5%20%D1%83%D1%81%D0%BB%D0%BE%D0%B2%D0%B8%D1%8F%20%D0%BF%D0%BE%20%D0%B2%D0%BA%D0%BB%D0%B0%D0%B4%D0%B0%D0%BC%20%D1%84%D0%B8%D0%B7%D0%B8%D1%87%D0%B5%D1%81%D0%BA%D0%B8%D1%85%20%D0%BB%D0%B8%D1%86%20%D1%81%2023.08.2025.pdf"
]

PCT_RE = re.compile(r"(\d{1,2}(?:[\.,]\d)?)\s*%")
TERM_RE = re.compile(r"(\d{2,4})\s*дн", re.I)
RANGE_RE = re.compile(r"(\d[\d\s]{2,})(?:\s*[–-]\s*(\d[\d\s]{2,}|∞))", re.I)


def normalize_num(s: str) -> float:
	s = (s or "").replace(" ", "").replace("\u00a0", "").replace(",", ".")
	return float(s)


def parse_amount_range(text: str) -> Optional[Tuple[float, Optional[float]]]:
	m = RANGE_RE.search(text.replace("\u00a0", " "))
	if not m:
		return None
	lo = normalize_num(m.group(1))
	hi_raw = m.group(2)
	hi = None if (not hi_raw or hi_raw.strip(" ∞") == "") else normalize_num(hi_raw)
	return (lo, hi)


def download(url: str, retries: int = 3, timeout: int = 45) -> str:
	h = {"User-Agent": "ai-bdm-rates/1.0"}
	last_err: Optional[Exception] = None
	for attempt in range(1, retries + 1):
		try:
			r = requests.get(url, timeout=timeout, headers=h)
			r.raise_for_status()
			fd, path = tempfile.mkstemp(suffix=".pdf")
			os.close(fd)
			with open(path, "wb") as f:
				f.write(r.content)
			return path
		except Exception as e:
			last_err = e
			time.sleep(2 * attempt)
	raise last_err  # type: ignore


def lines_from_pdf(path: str) -> List[Tuple[int, str]]:
	lines: List[Tuple[int, str]] = []
	with pdfplumber.open(path) as pdf:
		for i, page in enumerate(pdf.pages, start=1):
			text = page.extract_text() or ""
			for ln in (text.split("\n") if text else []):
				ln = ln.strip()
				if ln:
					lines.append((i, ln))
	return lines


def extract_rates_from_lines(lines: List[Tuple[int, str]], source_url: str, effective_from: str) -> List[Dict]:
	rows: List[Dict] = []
	ptype: Optional[str] = None
	current_ranges: List[Tuple[float, Optional[float]]] = []
	current_terms: List[int] = []
	# detect payout section by heading context
	for page, ln in lines:
		low = ln.lower()
		if "ежемесяч" in low:
			ptype = "monthly"
			current_ranges, current_terms = [], []
			continue
		if "в конце" in low or "по окончани" in low or "капитализац" in low:
			ptype = "end"
			current_ranges, current_terms = [], []
			continue
		# collect amount ranges present in header lines
		rg = parse_amount_range(ln)
		if rg and rg not in current_ranges:
			current_ranges.append(rg)
			continue
		# collect terms on term lines
		mt = TERM_RE.search(ln)
		if mt:
			val = int(mt.group(1))
			if val not in current_terms:
				current_terms.append(val)
			continue
		# cells with percents: try to map across current terms/ranges
		m = PCT_RE.findall(ln)
		if m and ptype and (current_terms and current_ranges):
			# naive mapping: iterate by order
			rates = [float(x.replace(',', '.')) for x in m]
			idx = 0
			for term in current_terms:
				for lo, hi in current_ranges:
					if idx >= len(rates):
						break
					rows.append({
						"product_code": "Вклад",
						"payout_type": ptype,
						"term_days": term,
						"amount_min": lo,
						"amount_max": hi,
						"amount_inclusive_end": True,
						"rate_percent": rates[idx],
						"channel": "Интернет-Банк",
						"effective_from": effective_from,
						"effective_to": None,
						"source_url": source_url,
						"source_page": page,
					})
					idx += 1
	return rows


def main():
	ap = argparse.ArgumentParser()
	ap.add_argument("--out", default="data/out_rates_deposit.json")
	ap.add_argument("--effective_from", default=dt.date.today().isoformat())
	ap.add_argument("--url", default=None, help="Single PDF URL to process")
	args = ap.parse_args()
	all_rows: List[Dict] = []
	urls = [args.url] if args.url else DEPOSIT_URLS
	for url in urls:
		if not url:
			continue
		print(f"Processing {url}")
		path = download(url)
		try:
			lines = lines_from_pdf(path)
			rows = extract_rates_from_lines(lines, url, args.effective_from)
			all_rows.extend(rows)
		finally:
			try:
				os.remove(path)
			except Exception:
				pass
	os.makedirs(os.path.dirname(args.out), exist_ok=True)
	with open(args.out, "w", encoding="utf-8") as f:
		json.dump(all_rows, f, ensure_ascii=False, indent=2)
	print(f"Saved {len(all_rows)} rows -> {args.out}")

if __name__ == "__main__":
	main() 