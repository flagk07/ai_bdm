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


@app.on_event("startup")
async def _set_commands() -> None:
	try:
		await bot.set_my_commands([BotCommand(command="menu", description="Показать меню")])
	except Exception:
		pass
	# Start periodic scheduler only if enabled via env
	if os.environ.get("NOTIFY_ENABLED", "").lower() in ("1", "true", "yes", "on"):  # default OFF
		async def push(chat_id: int, text: str) -> None:
			await bot.send_message(chat_id, text)
		StatsScheduler(db, push).start()


@app.get("/")
async def root() -> JSONResponse:
	return JSONResponse({"ok": True, "service": "ai-bdm"})


@app.get("/health")
async def health_plain() -> JSONResponse:
	return JSONResponse({"ok": True})


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