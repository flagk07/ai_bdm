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