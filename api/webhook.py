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