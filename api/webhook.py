from __future__ import annotations

import os
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from aiogram import Bot, Dispatcher
from aiogram.types import Update
from aiogram.fsm.storage.memory import MemoryStorage

from src.config import get_settings
from src.db import Database
from src.handlers import register_handlers

app = FastAPI()

# Initialize bot, db, dispatcher once (cold start)
settings = get_settings()
bot = Bot(token=settings.telegram_bot_token)
db = Database()
# Bootstrap allowed users from env (e.g., 195830791)
try:
	db.ensure_allowed_users_bootstrap(settings.allowed_tg_ids_bootstrap)
except Exception:
	pass

# Use in-memory storage for FSM
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
register_handlers(dp, db, bot, for_webhook=True)


# Root
@app.get("/")
async def root() -> JSONResponse:
	return JSONResponse({"ok": True, "service": "ai-bdm"})

# Health without prefix
@app.get("/health")
async def health_plain() -> JSONResponse:
	return JSONResponse({"ok": True})

# Health with /api prefix (Vercel routes)
@app.get("/api/health")
async def health_api() -> JSONResponse:
	return JSONResponse({"ok": True})

# Telegram webhook without prefix
@app.post("/webhook")
async def telegram_webhook_plain(request: Request) -> JSONResponse:
	payload = await request.json()
	try:
		db.log(None, "webhook_receive", payload)
	except Exception:
		pass
	update = Update.model_validate(payload)
	await dp.feed_update(bot, update)
	return JSONResponse({"ok": True})

# Telegram webhook with /api prefix
@app.post("/api/webhook")
async def telegram_webhook_api(request: Request) -> JSONResponse:
	payload = await request.json()
	try:
		db.log(None, "webhook_receive", payload)
	except Exception:
		pass
	update = Update.model_validate(payload)
	await dp.feed_update(bot, update)
	return JSONResponse({"ok": True}) 