from __future__ import annotations

import os
import asyncio
from datetime import date
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from aiogram import Bot, Dispatcher
from aiogram.types import Update, BotCommand
from aiogram.fsm.storage.memory import MemoryStorage

from src.config import get_settings
from src.db import Database
from src.handlers import register_handlers
from src.scheduler import StatsScheduler
# from src.rag import ingest_kn_docs, ingest_deposit_docs  # removed
from src.assistant import get_assistant_reply
import re
from typing import Dict

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
		app.state.scheduler = StatsScheduler(db, push)
		app.state.scheduler.start()
		try:
			db.log(None, "scheduler_start", {"ok": True})
		except Exception:
			pass


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
	return JSONResponse({"ok": False, "error": "disabled"}, status_code=410)


@app.post("/api/import_rates")
async def import_rates(request: Request) -> JSONResponse:
	return JSONResponse({"ok": False, "error": "disabled"}, status_code=410)


@app.post("/api/cleanup_deposits")
async def cleanup_deposits(request: Request) -> JSONResponse:
	return JSONResponse({"ok": False, "error": "disabled"}, status_code=410)


@app.post("/api/ingest_deposit_custom")
async def ingest_deposit_custom(request: Request) -> JSONResponse:
	return JSONResponse({"ok": False, "error": "disabled"}, status_code=410)


@app.post("/api/rag_reset_single")
async def rag_reset_single(request: Request) -> JSONResponse:
	return JSONResponse({"ok": False, "error": "disabled"}, status_code=410)


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
		city = request.query_params.get("city")
		if not city:
			try:
				payload = await request.json()
				city = (payload.get("city") or "").strip() if isinstance(payload, dict) else None
			except Exception:
				city = None
		def _do() -> None:
			row = {"tg_id": tg_id, "active": True}
			if city:
				row["city"] = city
			db.client.table("allowed_users").upsert(row, on_conflict="tg_id").execute()
			# Ensure employees row exists and carry city through
			emp = {"tg_id": tg_id}
			if city:
				emp["city"] = city
				# if we can derive timezone now — set it
				tz = _city_to_tz(city)
				if tz:
					emp["timezone"] = tz
			db.client.table("employees").upsert(emp, on_conflict="tg_id").execute()
		async def _run():
			await asyncio.to_thread(_do)
		await _run()
		return JSONResponse({"ok": True, "tg_id": tg_id, "city": city})
	except Exception as e:
		return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/assistant_test")
async def assistant_test(request: Request) -> JSONResponse:
	# Protected testing endpoint to ask assistant directly
	expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	try:
		payload = await request.json()
		text = (payload.get("text") or "").strip() if isinstance(payload, dict) else ""
		tg_id = int(payload.get("tg_id") or 195830791)
		if not text:
			return JSONResponse({"ok": False, "error": "missing text"}, status_code=400)
		# Ensure allowed user and employee exist
		try:
			db.client.table("allowed_users").upsert({"tg_id": tg_id, "active": True}, on_conflict="tg_id").execute()
			db.client.table("employees").upsert({"tg_id": tg_id}, on_conflict="tg_id").execute()
		except Exception:
			pass
		# Compute stats and call assistant
		today = date.today()
		stats = db.stats_day_week_month(tg_id, today)
		month_rank = db.month_ranking(today.replace(day=1), today)
		emp = db.get_or_register_employee(tg_id)
		agent_name = (emp.agent_name if emp else "Тест")
		answer = get_assistant_reply(db, tg_id, agent_name, stats, month_rank, text)
		return JSONResponse({"ok": True, "tg_id": tg_id, "question": text, "answer": answer})
	except Exception as e:
		try:
			db.log(None, "assistant_test_error", {"error": str(e)})
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

@app.get("/api/logs")
async def logs_api(request: Request) -> JSONResponse:
    expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
    token = request.query_params.get("token")
    if expected and token != expected:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    limit_raw = request.query_params.get("limit") or "30"
    try:
        limit = max(1, min(200, int(limit_raw)))
    except Exception:
        limit = 30
    try:
        q = db.client.table("logs").select("created_at, action, payload").order("created_at", desc=True).limit(limit)
        # optional action filter
        action_like = request.query_params.get("action_like")
        if action_like:
            # supabase python client lacks ilike on order chain; do a simple fetch and filter in python
            res = q.execute()
            data = getattr(res, "data", []) or []
            out = [r for r in data if action_like in (r.get("action") or "")]
        else:
            res = q.execute()
            out = getattr(res, "data", []) or []
        return JSONResponse({"ok": True, "logs": out})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/migrate_plans_penetration")
async def migrate_plans_penetration(request: Request) -> JSONResponse:
    expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
    token = request.query_params.get("token")
    if expected and token != expected:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        def _do() -> int:
            rows = db.client.table("sales_plans").select("tg_id,year,month,plan_month").execute()
            cnt = 0
            for r in (getattr(rows, "data", []) or []):
                tg = int(r.get("tg_id"))
                y = int(r.get("year"))
                m = int(r.get("month"))
                db.client.table("sales_plans").upsert({"tg_id": tg, "year": y, "month": m, "plan_month": 50}, on_conflict="tg_id,year,month").execute()
                cnt += 1
            return cnt
        cnt = await asyncio.to_thread(_do)
        return JSONResponse({"ok": True, "updated": cnt})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

def _city_to_tz(city: str | None) -> str | None:
    if not city:
        return None
    key = re.sub(r"\s+", " ", city.strip().lower())
    MAP = {
        "воронеж": "Europe/Moscow",
        "барнаул": "Asia/Barnaul",
        "иркутск": "Asia/Irkutsk",
        "калининград": "Europe/Kaliningrad",
        "кострома": "Europe/Moscow",
        "красноярск": "Asia/Krasnoyarsk",
        "кудрово": "Europe/Moscow",
        "екатеринбург": "Asia/Yekaterinburg",
        "москва": "Europe/Moscow",
        "нижневартовск": "Asia/Yekaterinburg",
        "омск": "Asia/Omsk",
        "оренбург": "Asia/Yekaterinburg",
        "казань": "Europe/Moscow",
        "ростов-на-дону": "Europe/Moscow",
        "набережные челны": "Europe/Moscow",
        "новосибирск": "Asia/Novosibirsk",
        "самара": "Europe/Samara",
        "санкт-петербург": "Europe/Moscow",
        "саратов": "Europe/Saratov" if False else "Europe/Samara",
        "псков": "Europe/Moscow",
        "тверь": "Europe/Moscow",
        "сургут": "Asia/Yekaterinburg",
        "тюмень": "Asia/Yekaterinburg",
        "улан-удэ": "Asia/Irkutsk",
        "хабаровск": "Asia/Vladivostok",
        "южно-сахалинск": "Asia/Sakhalin",
        "челябинск": "Asia/Yekaterinburg",
        "якутск": "Asia/Yakutsk",
        "мурманск": "Europe/Moscow",
        "владивосток": "Asia/Vladivostok",
    }
    return MAP.get(key)

@app.post("/api/backfill_city_timezone")
async def backfill_city_timezone(request: Request) -> JSONResponse:
    expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
    token = request.query_params.get("token")
    if expected and token != expected:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        def _do() -> Dict[str, int]:
            res = db.client.table("employees").select("tg_id, city, timezone").execute()
            upd = 0
            for r in (getattr(res, "data", []) or []):
                if r.get("timezone") and r.get("timezone") != "Europe/Moscow":
                    continue
                tz = _city_to_tz(r.get("city"))
                if tz:
                    db.client.table("employees").upsert({"tg_id": r["tg_id"], "timezone": tz}, on_conflict="tg_id").execute()
                    upd += 1
            return {"updated": upd}
        stats = await asyncio.to_thread(_do)
        return JSONResponse({"ok": True, **stats})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/backfill_city_copy")
async def backfill_city_copy(request: Request) -> JSONResponse:
    expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
    token = request.query_params.get("token")
    if expected and token != expected:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        def _do() -> Dict[str, int]:
            rows = db.client.table("allowed_users").select("tg_id, city").execute()
            copied = 0
            for r in (getattr(rows, "data", []) or []):
                city = (r.get("city") or "").strip()
                if not city:
                    continue
                # only set if employees.city is null or empty
                emp = db.client.table("employees").select("city").eq("tg_id", r["tg_id"]).maybe_single().execute()
                curr = (getattr(emp, "data", {}) or {}).get("city") if hasattr(emp, "data") else None
                if not curr:
                    db.client.table("employees").upsert({"tg_id": r["tg_id"], "city": city}, on_conflict="tg_id").execute()
                    copied += 1
            return {"copied": copied}
        stats = await asyncio.to_thread(_do)
        return JSONResponse({"ok": True, **stats})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/allowed_city_set_all")
async def allowed_city_set_all(request: Request) -> JSONResponse:
    expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
    token = request.query_params.get("token")
    if expected and token != expected:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    # city from query or body; default Москва
    city = request.query_params.get("city") or "Москва"
    try:
        def _do() -> Dict[str, int]:
            # fetch all allowed users
            res = db.client.table("allowed_users").select("tg_id").execute()
            rows = getattr(res, "data", []) or []
            if not rows:
                return {"updated": 0}
            updated = 0
            for r in rows:
                tg = int(r.get("tg_id"))
                db.client.table("allowed_users").upsert({"tg_id": tg, "city": city}, on_conflict="tg_id").execute()
                db.client.table("employees").upsert({"tg_id": tg, "city": city}, on_conflict="tg_id").execute()
                updated += 1
            return {"updated": updated, "city": city}
        out = await asyncio.to_thread(_do)
        return JSONResponse({"ok": True, **out})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/send_report_now")
async def send_report_now(request: Request) -> JSONResponse:
    expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
    token = request.query_params.get("token")
    if expected and token != expected:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        async def dummy_push(chat_id: int, text: str) -> None:
            return None
        sch = StatsScheduler(db, dummy_push)
        await sch._send_email_report()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/api/set_smtp")
async def set_smtp(request: Request) -> JSONResponse:
    expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
    token = request.query_params.get("token")
    if expected and token != expected:
        return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    try:
        payload = {}
        try:
            payload = await request.json()
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
        def _get(key: str, default: str = "") -> str:
            return (request.query_params.get(key) or str(payload.get(key) or default)).strip()
        # read values
        host = _get("SMTP_HOST")
        port = _get("SMTP_PORT")
        ssl = _get("SMTP_SSL")
        user = _get("SMTP_USER")
        pwd  = _get("SMTP_PASS")
        frm  = _get("EMAIL_FROM")
        to   = _get("EMAIL_TO")
        if host: os.environ["SMTP_HOST"] = host
        if port: os.environ["SMTP_PORT"] = port
        if ssl:  os.environ["SMTP_SSL"] = ssl
        if user: os.environ["SMTP_USER"] = user
        if pwd:  os.environ["SMTP_PASS"] = pwd
        if frm:  os.environ["EMAIL_FROM"] = frm
        if to:   os.environ["EMAIL_TO"] = to
        try:
            db.log(None, "set_smtp_runtime", {"host": host, "port": port, "ssl": ssl, "from": frm, "to": to and "masked"})
        except Exception:
            pass
        return JSONResponse({"ok": True, "applied": {"SMTP_HOST": host, "SMTP_PORT": port, "SMTP_SSL": ssl, "EMAIL_FROM": frm, "EMAIL_TO": to}})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.get("/api/scheduler_status")
async def scheduler_status(request: Request) -> JSONResponse:
	# Protected
	expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	try:
		sch = getattr(app.state, "scheduler", None)
		status = "missing" if sch is None else "present"
		jobs = []
		try:
			if sch is not None:
				jobs = [str(j.id) for j in sch.scheduler.get_jobs()] if hasattr(sch, "scheduler") else []
		except Exception:
			jobs = []
		return JSONResponse({"ok": True, "status": status, "jobs": jobs})
	except Exception as e:
		return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/api/scheduler_start")
async def scheduler_start(request: Request) -> JSONResponse:
	# Protected
	expected = os.environ.get("NOTIFY_TOKEN") or os.environ.get("RAG_TOKEN")
	token = request.query_params.get("token")
	if expected and token != expected:
		return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
	try:
		async def push(chat_id: int, text: str) -> None:
			await bot.send_message(chat_id, text)
		sch = getattr(app.state, "scheduler", None)
		if sch is None:
			app.state.scheduler = StatsScheduler(db, push)
			app.state.scheduler.start()
			try:
				db.log(None, "scheduler_start", {"ok": True, "manual": True})
			except Exception:
				pass
			return JSONResponse({"ok": True, "started": True})
		else:
			# Already present; try ensure started
			try:
				app.state.scheduler.scheduler.start()
				db.log(None, "scheduler_start", {"ok": True, "manual": True, "already": True})
			except Exception:
				pass
			return JSONResponse({"ok": True, "started": False, "already_present": True})
	except Exception as e:
		return JSONResponse({"ok": False, "error": str(e)}, status_code=500) 