from __future__ import annotations

import os
import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from aiogram import Bot, Dispatcher
from aiogram.types import Update

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

dp = Dispatcher()
register_handlers(dp, db, bot, for_webhook=True)


@app.get("/")
async def root() -> JSONResponse:
	return JSONResponse({"ok": True, "service": "ai-bdm"})


@app.get("/api/health")
async def health() -> JSONResponse:
	return JSONResponse({"ok": True})


@app.post("/api/webhook")
async def telegram_webhook(request: Request) -> JSONResponse:
	payload = await request.json()
	update = Update.model_validate(payload)
	await dp.feed_update(bot, update)
	return JSONResponse({"ok": True}) 