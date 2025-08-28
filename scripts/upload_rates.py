#!/usr/bin/env python3
import sys
import json
import os
import argparse
import requests

def main():
	p = argparse.ArgumentParser(description="Upload deposit product rates (FACTS) to API")
	p.add_argument("json_file", help="Path to JSON file with array of rate rows")
	p.add_argument("--url", default=os.environ.get("RATES_API_URL", "https://ai-bdm-bojc.onrender.com/api/import_rates"))
	p.add_argument("--token", default=os.environ.get("RATES_API_TOKEN"), help="NOTIFY_TOKEN/RAG_TOKEN for API")
	args = p.parse_args()
	if not args.token:
		print("Missing --token or RATES_API_TOKEN env", file=sys.stderr)
		sys.exit(1)
	with open(args.json_file, "r", encoding="utf-8") as f:
		data = json.load(f)
	r = requests.post(args.url, params={"token": args.token}, json=data, timeout=30)
	print(r.status_code, r.text)

if __name__ == "__main__":
	main() 