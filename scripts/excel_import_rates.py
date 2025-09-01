#!/usr/bin/env python3
import os
import sys
import argparse
import requests
import pandas as pd
from urllib.parse import urlparse

TOKEN = os.environ.get("RATES_API_TOKEN") or os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
API_URL = os.environ.get("RATES_API_URL", "https://ai-bdm-bojc.onrender.com/api/import_rates")

# Currency mapping from Excel values to canonical codes
CURRENCY_MAP = {
	"рубль": "RUB", "руб": "RUB", "rub": "RUB", "rur": "RUB", "ruble": "RUB", "₽": "RUB", "RUB": "RUB",
	"usd": "USD", "$": "USD", "доллар": "USD", "$ США": "USD", "USD": "USD",
	"eur": "EUR", "€": "EUR", "евро": "EUR", "EUR": "EUR",
	"cny": "CNY", "¥": "CNY", "юань": "CNY", "CNY": "CNY",
}

def normalize_currency(val: str) -> str:
	if val is None:
		return None
	s = str(val).strip().lower()
	return CURRENCY_MAP.get(s, None) or CURRENCY_MAP.get(s.replace(".", ""), None) or val


MONTHS_TO_DAYS = {1:31,2:61,3:91,4:122,6:181,9:274,12:367,18:550,24:730,36:1100}

def parse_term_days(cell) -> int:
	# Accept either integer days or strings like "181", "181 дней", "6 мес", etc.
	s = str(cell).strip().lower()
	import re
	# months
	if "мес" in s or "меся" in s:
		m = re.search(r"(\d+)", s)
		if m:
			mon = int(m.group(1))
			return MONTHS_TO_DAYS.get(mon, mon * 30)
	# days
	m = re.search(r"(\d+)", s)
	if m:
		d = int(m.group(1))
		return d if d > 0 else 0
	return 0


def parse_amount_min(cell) -> float:
	s = str(cell).replace("\u00a0", " ")
	s = s.replace(" ", "").replace(",", ".")
	# Allow ranges like ">=100000" or "от 1 000 000"
	import re
	m = re.search(r"(\d+(?:\.\d+)?)", s)
	return float(m.group(1)) if m else 0.0


def main():
	p = argparse.ArgumentParser(description="Import deposit rates from Excel into product_rates with amount_max calculation")
	p.add_argument("excel_url", help="Public URL to Excel file in Supabase Storage")
	p.add_argument("--token", default=TOKEN, help="API token (NOTIFY_TOKEN/RAG_TOKEN)")
	p.add_argument("--api", default=API_URL, help="Import API URL")
	p.add_argument("--channel", default=None)
	p.add_argument("--cleanup", action="store_true", help="Cleanup deposits before import")
	args = p.parse_args()

	if not args.token:
		print("Missing --token or RATES_API_TOKEN env", file=sys.stderr)
		sys.exit(1)

	# Optional cleanup
	if args.cleanup:
		clean_url = args.api.replace("/api/import_rates", "/api/cleanup_deposits")
		cr = requests.post(clean_url, params={"token": args.token}, timeout=60)
		print(cr.status_code, cr.text)

	# Download Excel to memory
	r = requests.get(args.excel_url, timeout=60)
	r.raise_for_status()
	with open("/tmp/_rates.xlsx", "wb") as f:
		f.write(r.content)

	df = pd.read_excel("/tmp/_rates.xlsx")
	# Forward-fill plan names to handle merged-like blanks
	if "Наименование" in df.columns:
		df["Наименование"] = df["Наименование"].ffill()
	# Expected columns: Наименование, Валюта вклада, Срок вклада, Пороговая сумма, Ставка
	col_map = {
		"Наименование": "plan_name",
		"Валюта вклада": "currency",
		"Срок вклада": "term_days",
		"Пороговая сумма": "amount_min",
		"Ставка": "rate_percent",
	}
	missing = [c for c in col_map if c not in df.columns]
	if missing:
		print(f"Missing columns: {missing}", file=sys.stderr)
		# Try alternative headers variants
		for alt in list(df.columns):
			pass

	# Normalize
	df_n = pd.DataFrame()
	df_n["plan_name"] = df["Наименование"].astype(str).str.strip()
	df_n["currency"] = df["Валюта вклада"].apply(normalize_currency)
	df_n["term_days"] = df["Срок вклада"].apply(parse_term_days)
	df_n["amount_min"] = df["Пороговая сумма"].apply(parse_amount_min)
	df_n["rate_percent"] = df["Ставка"].apply(lambda v: float(str(v).replace(",", ".")))
	df_n["payout_type"] = "monthly"  # per user instruction
	if args.channel:
		df_n["channel"] = args.channel
	# keep any positive term_days (no restriction)
	df_n = df_n[df_n["term_days"] > 0].copy()

	# Calculate amount_max within each (plan_name, currency, term_days)
	rows = []
	for (plan, curr, term), g in df_n.groupby(["plan_name", "currency", "term_days"], dropna=False):
		g2 = g.sort_values("amount_min").reset_index(drop=True)
		for i, row in g2.iterrows():
			amount_min_val = float(row["amount_min"]) if pd.notna(row["amount_min"]) else 0.0
			rate_val = float(row["rate_percent"]) if pd.notna(row["rate_percent"]) else float("nan")
			from math import isfinite
			if not isfinite(amount_min_val):
				continue
			if not isfinite(rate_val) or rate_val <= 0 or rate_val > 100:
				continue
			next_min = float(g2.loc[i+1, "amount_min"]) if i + 1 < len(g2) and pd.notna(g2.loc[i+1, "amount_min"]) else None
			amount_max_val = (next_min - 0.01) if (next_min is not None and isfinite(next_min)) else None
			rows.append({
				"product_code": "Вклад",
				"plan_name": plan,
				"currency": curr,
				"term_days": int(term),
				"payout_type": "monthly",
				"amount_min": amount_min_val,
				"amount_max": amount_max_val,
				"amount_inclusive_end": True,
				"rate_percent": rate_val,
				"channel": args.channel,
				"source_url": None,
				"source_page": None,
			})

	# POST to API in chunks
	import math, json
	from math import isfinite
	BATCH = 500
	for i in range(0, len(rows), BATCH):
		batch = rows[i:i+BATCH]
		# sanitize NaN/inf for JSON
		for r in batch:
			if r["amount_max"] is not None and not isfinite(float(r["amount_max"])):
				r["amount_max"] = None
			if r["rate_percent"] is not None and not isfinite(float(r["rate_percent"])):
				r["rate_percent"] = None
		resp = requests.post(args.api, params={"token": args.token}, json=batch, timeout=120)
		print(resp.status_code, resp.text)
		if resp.status_code >= 300:
			print("Upload failed", file=sys.stderr)
			sys.exit(2)

	print(f"Imported {len(rows)} rows")

if __name__ == "__main__":
	main() 