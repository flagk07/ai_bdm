from __future__ import annotations

import os
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from aiogram import Bot, Dispatcher
from aiogram.types import Update, BotCommand
from aiogram.fsm.storage.memory import MemoryStorage

from src.config import get_settings
from src.db import Database
from src.handlers import register_handlers
from src.scheduler import StatsScheduler
from src.rag import ingest_kn_docs, ingest_deposit_docs

app = FastAPI()

# Initialize bot, db, dispatcher once (cold start)
settings = get_settings()
bot = Bot(token=settings.telegram_bot_token)
db = Database()
try:
	db.ensure_allowed_users_bootstrap(settings.allowed_tg_ids_bootstrap)
except Exception:
	pass

storage = MemoryStorage()
dp = Dispatcher(storage=storage)
register_handlers(dp, db, bot, for_webhook=True)


def _env_off(val: str | None) -> bool:
	if not val:
		return False
	return val.lower() in ("0", "false", "no", "off")


@app.on_event("startup")
async def _set_commands() -> None:
	try:
		await bot.set_my_commands([BotCommand(command="menu", description="Показать меню")])
	except Exception:
		pass
	# Best-effort RAG ingest for KN in background (non-blocking)
	try:
		async def _bg_ingest():
			try:
				cnt = await asyncio.to_thread(ingest_kn_docs, db)
				try:
					db.log(None, "rag_ingest_kn", {"count": cnt, "bg": True})
				except Exception:
					pass
			except Exception:
				pass
		asyncio.create_task(_bg_ingest())
	except Exception:
		pass
	# Auto-set Telegram webhook if WEBHOOK_URL or RENDER_EXTERNAL_URL provided
	try:
		base = os.environ.get("WEBHOOK_URL") or os.environ.get("RENDER_EXTERNAL_URL")
		if base:
			url = base.rstrip('/')
			if not url.endswith('/webhook'):
				url = url + '/webhook'
			await bot.set_webhook(url, drop_pending_updates=True)
			try:
				db.log(None, "set_webhook", {"url": url, "ok": True})
			except Exception:
				pass
			# Verify and log webhook info
			try:
				info = await bot.get_webhook_info()
				payload = {"url": info.url, "pending": info.pending_update_count}
				try:
					db.log(None, "webhook_info", payload)
				except Exception:
					pass
			except Exception:
				pass
	except Exception as e:
		try:
			db.log(None, "set_webhook_error", {"error": str(e)})
		except Exception:
			pass
	# Start periodic scheduler by default; allow disabling via NOTIFY_ENABLED=0/false/no/off
	if not _env_off(os.environ.get("NOTIFY_ENABLED")):
		async def push(chat_id: int, text: str) -> None:
			await bot.send_message(chat_id, text)
		StatsScheduler(db, push).start()


@app.get("/")
async def root() -> JSONResponse:
	return JSONResponse({"ok": True, "service": "ai-bdm"})


@app.get("/health")
async def health_plain() -> JSONResponse:
	return JSONResponse({"ok": True})


@app.get("/api/health")
async def health_api() -> JSONResponse:
	info = None
	try:
		wi = await bot.get_webhook_info()
		info = {"url": wi.url, "pending": wi.pending_update_count}
	except Exception as e:
		info = {"error": str(e)}
	return JSONResponse({"ok": True, "webhook": info})


@app.post("/api/ingest_rag")
@app.get("/api/ingest_rag")
async def ingest_rag(request: Request) -> JSONResponse:
	# Optional token protection
	expected = os.environ.get("RAG_TOKEN") or os.environ.get("NOTIFY_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	# product selector
	product = (request.query_params.get("product") or "kn").lower()
	# Run ingest in thread to avoid blocking loop too long
	try:
		async def _run_ingest(which: str) -> dict:
			def _do() -> dict:
				res = {"kn": 0, "deposit": 0}
				if which in ("kn", "all"):
					res["kn"] = ingest_kn_docs(db)
				if which in ("deposit", "all"):
					res["deposit"] = ingest_deposit_docs(db)
				return res
			return await asyncio.to_thread(_do)
		resmap = await _run_ingest("all" if product == "all" else ("deposit" if product.startswith("dep") else "kn"))
		# quick counts
		try:
			docs = db.client.table("rag_docs").select("id").execute()
			chunks = db.client.table("rag_chunks").select("id").execute()
			dc = len(getattr(docs, "data", []) or [])
			cc = len(getattr(chunks, "data", []) or [])
		except Exception:
			dc, cc = None, None
		return JSONResponse({"ok": True, "ingested": resmap, "docs": dc, "chunks": cc})
	except Exception as e:
		return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/add_allowed")
@app.get("/api/add_allowed")
async def add_allowed(request: Request) -> JSONResponse:
	# Reuse NOTIFY_TOKEN or RAG_TOKEN as admin token
	expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	# Accept tg_id from query or JSON body
	tg_raw = request.query_params.get("tg_id")
	if not tg_raw:
		try:
			payload = await request.json()
			tg_raw = str(payload.get("tg_id")) if isinstance(payload, dict) else None
		except Exception:
			tg_raw = None
	if not tg_raw:
		return JSONResponse({"ok": False, "error": "missing tg_id"}, status_code=400)
	try:
		tg_id = int(tg_raw)
	except Exception:
		return JSONResponse({"ok": False, "error": "invalid tg_id"}, status_code=400)
	# Upsert into allowed_users and pre-create employee row (idempotent)
	try:
		def _do() -> None:
			db.client.table("allowed_users").upsert({"tg_id": tg_id, "active": True}, on_conflict="tg_id").execute()
			# Optional: ensure employees row exists so /start proceeds smoothly
			db.client.table("employees").upsert({"tg_id": tg_id}, on_conflict="tg_id").execute()
		async def _run():
			await asyncio.to_thread(_do)
		await _run()
		return JSONResponse({"ok": True, "tg_id": tg_id})
	except Exception as e:
		return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/import_rates")
async def import_rates(request: Request) -> JSONResponse:
	# Token protection
	expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	# Parse JSON array of rows
	try:
		payload = await request.json()
		if not isinstance(payload, list):
			return JSONResponse({"ok": False, "error": "expected JSON array"}, status_code=400)
		rows: list[dict] = []
		for item in payload:
			if not isinstance(item, dict):
				continue
			row: dict = {
				"product_code": item.get("product_code") or "Вклад",
				"payout_type": item.get("payout_type"),
				"term_days": int(item.get("term_days")),
				"amount_min": float(item.get("amount_min")),
				"amount_max": (float(item.get("amount_max")) if item.get("amount_max") is not None else None),
				"amount_inclusive_end": bool(item.get("amount_inclusive_end", True)),
				"rate_percent": float(item.get("rate_percent")),
				"channel": item.get("channel"),
				"effective_from": item.get("effective_from"),
				"effective_to": item.get("effective_to"),
				"source_url": item.get("source_url"),
				"source_page": (int(item.get("source_page")) if item.get("source_page") is not None else None),
			}
			# optional currency
			if item.get("currency") is not None:
				row["currency"] = item.get("currency")
			# optional plan_name
			if item.get("plan_name") is not None:
				row["plan_name"] = item.get("plan_name")
			rows.append(row)
		# Upsert (insert) facts
		await asyncio.to_thread(db.product_rates_upsert, rows)
		return JSONResponse({"ok": True, "inserted": len(rows)})
	except Exception as e:
		return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


@app.post("/api/cleanup_deposits")
async def cleanup_deposits(request: Request) -> JSONResponse:
	# Token protection
	expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	try:
		def _do() -> dict:
			# delete product_rates for deposits
			try:
				db.client.table("product_rates").delete().eq("product_code", "Вклад").execute()
			except Exception:
				pass
			# delete rag_docs and related rag_chunks for deposits
			try:
				# find deposit docs
				docs = db.client.table("rag_docs").select("id").eq("product_code", "Вклад").execute()
				ids = [r["id"] for r in (getattr(docs, "data", []) or [])]
				for did in ids:
					try:
						db.client.table("rag_chunks").delete().eq("doc_id", did).execute()
					except Exception:
						pass
				# delete docs
				db.client.table("rag_docs").delete().eq("product_code", "Вклад").execute()
			except Exception:
				pass
			return {"rates_deleted": True, "docs_deleted": True}
		res = await asyncio.to_thread(_do)
		return JSONResponse({"ok": True, **(res or {})})
	except Exception as e:
		return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/ingest_deposit_custom")
async def ingest_deposit_custom(request: Request) -> JSONResponse:
	# Token protection
	expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	# Parse body
	try:
		payload = await request.json()
		urls = payload.get("urls") if isinstance(payload, dict) else None
		if not urls or not isinstance(urls, list):
			return JSONResponse({"ok": False, "error": "expected {\"urls\":[...]}"}, status_code=400)
	except Exception as e:
		try:
			db.log(None, "ingest_deposit_custom_exception", {"error": str(e)})
		except Exception:
			pass
		return JSONResponse({"ok": False, "error": str(e)}, status_code=400)
	# Log start
	try:
		db.log(None, "ingest_deposit_custom_start", {"count": len(urls)})
	except Exception:
		pass
	# Define worker
	try:
		def _do() -> dict:
			import src.rag as rag_mod
			count = 0
			for u in urls:
				try:
					mime, title, text = rag_mod._fetch_text_from_url(u)
					if not text:
						continue
					row = {"url": u, "title": title, "product_code": "Вклад", "mime": mime, "content": text}
					# upsert doc
					doc_id = None
					try:
						ins = db.client.table("rag_docs").upsert(row, on_conflict="url").select("id").eq("url", u).maybe_single().execute()
						doc = getattr(ins, "data", None)
						if doc and doc.get("id"):
							doc_id = doc["id"]
					except Exception:
						try:
							db.client.table("rag_docs").delete().eq("url", u).execute()
							db.client.table("rag_docs").insert(row).execute()
							sel = db.client.table("rag_docs").select("id").eq("url", u).single().execute()
							doc_id = getattr(sel, "data", {}).get("id")
						except Exception:
							continue
					if not doc_id:
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
					# sections/chunks for rules
					sections = rag_mod._extract_deposit_rule_sections(text)
					parts = rag_mod._pack_rule_sections_to_chunks(sections)
					embeds = rag_mod._embed_texts(parts)
					bulk = []
					for idx, part in enumerate(parts):
						if not part.strip():
							continue
						rowc = {"doc_id": doc_id, "product_code": "Вклад", "chunk_index": idx, "content": part}
						cur = rag_mod._infer_currency(part)
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
			# parse rates from the first URL only (best-effort)
			try:
				first_url = urls[0]
				mime, title, text = rag_mod._fetch_text_from_url(first_url)
				import re as _re
				lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
				rows: list[dict] = []
				current_payout = None
				for ln in lines:
					low = ln.lower()
					if "ежемесячно" in low:
						current_payout = "monthly"
					elif "в конце срока" in low:
						current_payout = "end"
					m = _re.findall(r"(61|91|122|181|274|367|550|730|1100).*?(\d{1,2}[\.,]\d)\s*%.*?(\d{1,2}[\.,]\d)\s*%", low)
					if m and current_payout:
						term = int(m[0][0])
						rate1 = float(m[0][1].replace(',', '.'))
						rate2 = float(m[0][2].replace(',', '.'))
						rows.append({
							"product_code": "Вклад",
							"payout_type": current_payout,
							"term_days": term,
							"amount_min": 30000.0,
							"amount_max": 999999.99,
							"amount_inclusive_end": True,
							"rate_percent": rate1,
							"channel": None,
							"effective_from": None,
							"effective_to": None,
							"source_url": first_url,
							"source_page": None,
						})
						rows.append({
							"product_code": "Вклад",
							"payout_type": current_payout,
							"term_days": term,
							"amount_min": 1000000.0,
							"amount_max": 15000000.0,
							"amount_inclusive_end": True,
							"rate_percent": rate2,
							"channel": None,
							"effective_from": None,
							"effective_to": None,
							"source_url": first_url,
							"source_page": None,
						})
				if rows:
					db.product_rates_upsert(rows)
			except Exception:
				pass
			return {"docs": count}
		# Start background job
		async def _run_bg() -> None:
			try:
				res = await asyncio.to_thread(_do)
				try:
					db.log(None, "ingest_deposit_custom_done", res)
				except Exception:
					pass
			except Exception as e:
				try:
					db.log(None, "ingest_deposit_custom_error", {"error": str(e)})
				except Exception:
					pass
		try:
			asyncio.create_task(_run_bg())
		except Exception:
			pass
		return JSONResponse({"ok": True, "started": True, "count_urls": len(urls)})
	except Exception as e:
		try:
			db.log(None, "ingest_deposit_custom_exception", {"error": str(e)})
		except Exception:
			pass
		return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/webhook")
async def telegram_webhook(request: Request) -> JSONResponse:
	payload = await request.json()
	try:
		db.log(None, "webhook_receive", payload)
	except Exception:
		pass
	update = Update.model_validate(payload)
	await dp.feed_update(bot, update)
	return JSONResponse({"ok": True})


@app.get("/api/diag")
async def diag(request: Request) -> JSONResponse:
	# Token protection
	expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	# Optional tg_id
	tg_raw = request.query_params.get("tg_id")
	try:
		wi = await bot.get_webhook_info()
		wh = {"url": wi.url, "pending": wi.pending_update_count}
	except Exception as e:
		wh = {"error": str(e)}
	allowed = None
	emp = None
	msgs = None
	last_logs = []
	if tg_raw:
		try:
			uid = int(tg_raw)
			try:
				row = db.client.table("allowed_users").select("tg_id,active").eq("tg_id", uid).maybe_single().execute()
				allowed = getattr(row, "data", None)
			except Exception:
				allowed = None
			try:
				row2 = db.client.table("employees").select("tg_id,agent_name,active").eq("tg_id", uid).maybe_single().execute()
				emp = getattr(row2, "data", None)
			except Exception:
				emp = None
			try:
				m = db.client.table("assistant_messages").select("id").eq("tg_id", uid).limit(3).execute()
				msgs = len(getattr(m, "data", []) or [])
			except Exception:
				msgs = None
		except Exception:
			pass
	# last webhook logs
	try:
		lg = db.client.table("logs").select("created_at, action").order("created_at", desc=True).limit(5).execute()
		last_logs = getattr(lg, "data", []) or []
	except Exception:
		last_logs = []
	return JSONResponse({"ok": True, "webhook": wh, "allowed": allowed, "employee": emp, "messages_recent": msgs, "logs": last_logs}) 