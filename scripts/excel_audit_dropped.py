#!/usr/bin/env python3
import argparse
import pandas as pd
import requests

ALLOWED_DAYS = {61,91,122,181,274,367,550,730,1100}
MONTHS_TO_DAYS = {2:61,3:91,4:122,6:181,9:274,12:367,18:550,24:730,36:1100}

CURRENCY_MAP = {
	"рубль": "RUB", "руб": "RUB", "rub": "RUB", "rur": "RUB", "ruble": "RUB", "₽": "RUB", "RUB": "RUB",
	"usd": "USD", "$": "USD", "доллар": "USD", "$ США": "USD", "USD": "USD",
	"eur": "EUR", "€": "EUR", "евро": "EUR", "EUR": "EUR",
	"cny": "CNY", "¥": "CNY", "юань": "CNY", "CNY": "CNY",
}

def normalize_currency(val: str):
	if val is None:
		return None
	s = str(val).strip().lower()
	return CURRENCY_MAP.get(s, None) or CURRENCY_MAP.get(s.replace(".", ""), None) or val

def parse_term_days(cell) -> int:
	s = str(cell).strip().lower()
	import re
	if "мес" in s or "меся" in s:
		m = re.search(r"(\d+)", s)
		if m:
			mon = int(m.group(1))
			return MONTHS_TO_DAYS.get(mon, 0)
	m = re.search(r"(\d+)", s)
	if m:
		d = int(m.group(1))
		return d if d in ALLOWED_DAYS else 0
	return 0

def parse_amount_min(cell) -> float:
	s = str(cell).replace("\u00a0", " ")
	s = s.replace(" ", "").replace(",", ".")
	import re
	m = re.search(r"(\d+(?:\.\d+)?)", s)
	return float(m.group(1)) if m else float("nan")

def main():
	p = argparse.ArgumentParser(description="Audit dropped Excel rows and reasons")
	p.add_argument("excel_url")
	args = p.parse_args()
	resp = requests.get(args.excel_url, timeout=60)
	resp.raise_for_status()
	with open("/tmp/_rates.xlsx", "wb") as f:
		f.write(resp.content)
	df = pd.read_excel("/tmp/_rates.xlsx")
	if "Наименование" in df.columns:
		df["Наименование"] = df["Наименование"].ffill()
	rows = []
	for idx, r in df.iterrows():
		plan = str(r.get("Наименование", "")).strip()
		curr = normalize_currency(r.get("Валюта вклада"))
		term = parse_term_days(r.get("Срок вклада"))
		amin = parse_amount_min(r.get("Пороговая сумма"))
		try:
			rate = float(str(r.get("Ставка")).replace(",", "."))
		except Exception:
			rate = float("nan")
		reasons = []
		if term == 0:
			reasons.append("невалидный срок (не из допустимых)")
		if not pd.notna(amin):
			reasons.append("невалидный amount_min")
		if not pd.notna(rate) or rate <= 0 or rate > 100:
			reasons.append("невалидная ставка")
		if reasons:
			rows.append({"row_index": int(idx)+2, "plan_name": plan, "currency": curr, "raw_term": r.get("Срок вклада"), "parsed_term_days": term, "amount_min": r.get("Пороговая сумма"), "rate": r.get("Ставка"), "reasons": "; ".join(reasons)})
	if not rows:
		print("No dropped rows")
		return
	out = pd.DataFrame(rows)
	print(out.to_csv(index=False))

if __name__ == "__main__":
	main() 