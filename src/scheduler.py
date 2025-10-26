from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Callable, Dict, Tuple, List
import json
import re

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from .config import get_settings
from .db import Database, count_workdays
from .assistant import get_assistant_reply
import io
import smtplib
from email.message import EmailMessage
try:
    import pandas as pd  # type: ignore
except Exception:
    pd = None

# deploy bump: ensure latest commit triggers Render auto-deploy


class StatsScheduler:
	def __init__(self, db: Database, push_func: Callable[[int, str], None]):
		self.db = db
		self.push_func = push_func
		self.scheduler = AsyncIOScheduler(timezone=pytz.timezone(get_settings().timezone))

	def _week_range(self, day: date) -> Tuple[date, date]:
		start = day - timedelta(days=day.weekday())
		return start, day

	def _prev_week_range(self, day: date) -> Tuple[date, date]:
		start_this, _ = self._week_range(day)
		end_prev = start_this - timedelta(days=1)
		start_prev = end_prev - timedelta(days=end_prev.weekday())
		return start_prev, end_prev

	def _month_range(self, day: date) -> Tuple[date, date]:
		return day.replace(day=1), day

	def _prev_month_range(self, day: date) -> Tuple[date, date]:
		first = day.replace(day=1)
		end_prev = first - timedelta(days=1)
		start_prev = end_prev.replace(day=1)
		return start_prev, end_prev

	def _delta_pct(self, current: int, previous: int) -> int:
		base = previous if previous > 0 else 1
		return int(round((current - previous) * 100 / base))

	def _format_delta(self, d: int) -> str:
		# Show explicit + only for positive values; zero stays as 0
		return f"+{d}" if d > 0 else (f"{d}" if d < 0 else "0")

	def _shape_ai_comment(self, text: str) -> str:
		"""Force required 1)/- bullet format and strip accidental stats blocks.
		- Numbered points (1), 2), ...) always on new lines; convert legacy '1.' to '1)'.
		- Sub-bullets start with '- ' on new lines.
		- If a numbered point has exactly one immediate sub-bullet, inline it without '-'.
		"""
		if not text:
			return ""
		norm = text.replace("\r\n", "\n").replace("\r", "\n")
		# Ensure newlines before numbered bullets (both 1) and 1.)
		norm = re.sub(r"(?<!^)\s+(?=\d{1,2}\)\s)", "\n", norm)
		norm = re.sub(r"(?<!^)\s+(?=\d{1,2}\.\s)", "\n", norm)
		# Ensure newlines before '- '
		norm = re.sub(r"(?<!^)\s+(?=-\s)", "\n", norm)
		# Split and clean
		raw_lines = [ln.strip() for ln in norm.split("\n") if ln.strip()]
		lines: List[str] = []
		for raw in raw_lines:
			# drop stats-like lines
			if re.match(r"^\d+\.\s*(Период|Итого попыток|По продуктам|Лидеры группы)\b", raw, re.IGNORECASE):
				continue
			# convert 1. -> 1)
			raw = re.sub(r"^(\d{1,2})\.\s+", r"\1) ", raw)
			lines.append(raw)
		# Inline single sub-bullet
		result: List[str] = []
		i = 0
		while i < len(lines):
			line = lines[i]
			m = re.match(r"^(\d{1,2}\))\s+(.*)", line)
			if m and i + 1 < len(lines):
				m_sub = re.match(r"^-\s+(.*)", lines[i + 1])
				is_single = False
				if m_sub:
					if (i + 2 >= len(lines)) or re.match(r"^(\d{1,2})[)\.]\s+", lines[i + 2]):
						is_single = True
				if m_sub and is_single:
					result.append(f"{m.group(1)} {m.group(2)} {m_sub.group(1)}")
					i += 2
					continue
			result.append(line)
			i += 1
		return "\n".join(result).strip()

	def _fmt1(self, val: float | int) -> str:
		try:
			v = float(val)
			if abs(v - round(v)) < 1e-9:
				return str(int(round(v)))
			return f"{v:.1f}".replace('.', ',')
		except Exception:
			return str(val)

	def _coach_lines(self, today_by: Dict[str, int], d_day: int, d_week: int, d_month: int) -> List[str]:
		lines: List[str] = []
		# 1) Если спад сегодня — уточнить причину и предложить действие
		if d_day < 0:
			lines.append(f"1. Спад сегодня (Δ {d_day}%). Что мешает: трафик, отказ, скрипт?")
			lines.append("2. Действие: сделайте 3 доп. попытки по сильному продукту до конца дня.")
		# 2) Если неделя проседает — план
		elif d_week < 0:
			lines.append(f"1. Неделя проседает (Δ {d_week}%). Где теряем: первая встреча или дожим?")
			lines.append("2. Действие: добавьте 5 целевых предложений на следующей смене.")
		# 3) Если месяц проседает — пересбор плана
		elif d_month < 0:
			lines.append(f"1. Месяц ниже темпа (Δ {d_month}%).")
			lines.append("2. Действие: пересоберите план по 2 продуктам с наибольшей воронкой.")
		# 4) Иначе — усиление сильной стороны и расширение
		else:
			# Найти сильный продукт сегодня
			top = None
			if today_by:
				top = max(today_by.items(), key=lambda x: x[1])
			if top and top[1] > 0:
				lines.append(f"1. Сильная сторона: {top[0]} — продолжайте в том же темпе.")
				lines.append("2. Действие: добавьте смежный продукт в каждый диалог.")
			else:
				lines.append("1. Сегодня ещё нет попыток — начните с 3 быстрых предложений по ключевому продукту.")
				lines.append("2. Действие: используйте короткий скрипт открытия и уточняющий вопрос.")
		return lines

	def _env_on(self, val: str | None) -> bool:
		if not val:
			return False
		return val.lower() in ("1", "true", "yes", "on")

	# Removed legacy _send_daily/_send_periodic to avoid unintended telegram pings (e.g., 20:00)

	def start(self) -> None:
		# Flags
		notify_enabled = os.environ.get("NOTIFY_ENABLED", "1").lower() not in ("0","false","no","off")
		email_enabled = os.environ.get("EMAIL_REPORT_ENABLED", "1").lower() not in ("0","false","no","off")
		# Auto-summary policy: once per day at 13:00 local time (if work session is open)
		# Always register the job; the worker will check NOTIFY_ENABLED at runtime
		self.scheduler.add_job(self._autosum_13_worker, CronTrigger(minute="*"))
		# Email report at 11:00 and 19:00 Moscow time
		if email_enabled:
			self.scheduler.add_job(self._send_email_report, CronTrigger(hour="11,19", minute=0))
			# Fallback checker every minute: if missed today after scheduled time, send once
			self.scheduler.add_job(self._email_report_fallback_worker, CronTrigger(minute="*"))
		# Usability report at 08:00 Moscow time (daily) — always register
		self.scheduler.add_job(self._send_usability_report, CronTrigger(hour=8, minute=0, timezone=pytz.timezone("Europe/Moscow")))
		# Auto-close workday at 21:00 local time via minute worker
		self.scheduler.add_job(self._autoclose_21_worker, CronTrigger(minute="*"))
		self.scheduler.start() 

	async def _autosum_13_worker(self) -> None:
		"""Every minute: for each active employee, if local time is 13:00 and work is open, send autosummary once per day."""
		# respect NOTIFY_ENABLED at runtime
		if os.environ.get("NOTIFY_ENABLED", "1").lower() in ("0","false","no","off"):
			return
		day_utc = datetime.utcnow().date()
		try:
			emps = self.db.list_active_employees()
		except Exception:
			emps = []
		for r in (emps or []):
			try:
				tg = int(r.get("tg_id"))
			except Exception:
				continue
			# Check session open
			if not self.db.work_is_open(tg):
				continue
			# Local time check
			tz_name = (r.get("timezone") or get_settings().timezone)
			try:
				tz = pytz.timezone(tz_name)
			except Exception:
				tz = pytz.timezone(get_settings().timezone)
			now_local = datetime.now(tz)
			if not (now_local.hour == 13):
				continue
			# Avoid duplicates: check if already sent today
			try:
				rec = self.db.client.table("logs").select("created_at").eq("tg_id", tg).eq("action", "autosum_13_sent").order("created_at", desc=True).limit(1).execute()
				rows = getattr(rec, "data", []) or []
				if rows:
					created = rows[0].get("created_at")
					dt = datetime.fromisoformat(str(created).replace("Z", "+00:00")).astimezone(tz)
					if dt.date() == now_local.date():
						continue
			except Exception:
				pass
			# Build and send summary (reuse logic from _send_periodic)
			today = now_local.date()
			start_week = today - timedelta(days=today.weekday())
			start_month = today.replace(day=1)
			name = r.get("agent_name") or f"agent?{tg}"
			# Totals
			today_total, today_by = self.db._sum_attempts_query(tg, today, today)
			week_total, _ = self.db._sum_attempts_query(tg, start_week, today)
			month_total, _ = self.db._sum_attempts_query(tg, start_month, today)
			# meetings and penetration
			m_day = self.db.meets_period_count(tg, today, today)
			m_week = self.db.meets_period_count(tg, start_week, today)
			m_month = self.db.meets_period_count(tg, start_month, today)
			linked_day = self.db.attempts_linked_period_count(tg, today, today)
			linked_week = self.db.attempts_linked_period_count(tg, start_week, today)
			linked_month = self.db.attempts_linked_period_count(tg, start_month, today)
			pen_day = (linked_day * 100 / m_day) if m_day > 0 else 0
			pen_week = (linked_week * 100 / m_week) if m_week > 0 else 0
			pen_month = (linked_month * 100 / m_month) if m_month > 0 else 0
			# target and deltas
			pen_target = int(self.db.compute_plan_breakdown(tg, today).get('penetration_target_pct', 50))
			start_prev_w = start_week - timedelta(days=7)
			end_prev_w = start_week - timedelta(days=1)
			m_prev_day = self.db.meets_period_count(tg, today - timedelta(days=1), today - timedelta(days=1))
			linked_prev_day = self.db.attempts_linked_period_count(tg, today - timedelta(days=1), today - timedelta(days=1))
			pen_prev_day = (linked_prev_day * 100 / m_prev_day) if m_prev_day > 0 else 0
			m_prev_week = self.db.meets_period_count(tg, start_prev_w, end_prev_w)
			linked_prev_week = self.db.attempts_linked_period_count(tg, start_prev_w, end_prev_w)
			pen_prev_week = (linked_prev_week * 100 / m_prev_week) if m_prev_week > 0 else 0
			end_prev_m = start_month - timedelta(days=1)
			start_prev_m = end_prev_m.replace(day=1)
			m_prev_month = self.db.meets_period_count(tg, start_prev_m, end_prev_m)
			linked_prev_month = self.db.attempts_linked_period_count(tg, start_prev_m, end_prev_m)
			pen_prev_month = (linked_prev_month * 100 / m_prev_month) if m_prev_month > 0 else 0
			def _delta_pp(a: float, b: float) -> int:
				return int(round(a - b))
			d_pen_day = _delta_pp(int(round(pen_day)), int(round(pen_prev_day)))
			d_pen_week = _delta_pp(int(round(pen_week)), int(round(pen_prev_week)))
			d_pen_month = _delta_pp(int(round(pen_month)), int(round(pen_prev_month)))
			# breakdown
			items = [(p, c) for p, c in (today_by or {}).items() if c > 0]
			items.sort(key=lambda x: (-x[1], x[0]))
			breakdown = ", ".join([f"{c}{p}" for p, c in items]) if items else "—"
			lines = []
			lines.append(f"- Сегодня: {int(round(pen_day))}% ({today_total}шт.) факт / {pen_target}% план / {int(round((pen_day/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_day}%")
			lines.append(f"- Сегодня по продуктам: {breakdown}")
			lines.append(f"- Неделя: {int(round(pen_week))}% ({week_total}шт.) факт / {pen_target}% план / {int(round((pen_week/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_week}%")
			lines.append(f"- Месяц: {int(round(pen_month))}% ({month_total}шт.) факт / {pen_target}% план / {int(round((pen_month/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_month}%")
			text = f"{name} — авто‑сводка\n" + "\n".join(lines) + "\n"
			# AI comment
			stats_dwm = self.db.stats_day_week_month(tg, today)
			month_rank = self.db.month_ranking(start_month, today)
			comment = get_assistant_reply(self.db, tg, name, stats_dwm, month_rank, "[AUTO_SUMMARY_13] Оцени результаты и дай рекомендации")
			await self.push_func(tg, text + self._shape_ai_comment(comment) + "\n")
			# log sent
			try:
				self.db.log(tg, "autosum_13_sent", {"hour": 13})
			except Exception:
				pass

	async def _send_email_report(self) -> None:
		# Build and send USABILITY report (requested format), but keep the same endpoint
		settings = get_settings()
		if not settings.smtp_host or not settings.smtp_user or not settings.smtp_pass:
			try:
				self.db.log(None, "email_report_skip", {"reason": "smtp_not_configured"})
			except Exception:
				pass
			return
		if not getattr(settings, "email_to_csv", ""):
			try:
				self.db.log(None, "email_report_skip", {"reason": "recipient_not_set"})
			except Exception:
				pass
			return
		# recipients come from settings (we already set to two emails at runtime)
		recipients = [addr.strip() for addr in str(settings.email_to_csv).split(',') if addr.strip()]
		# compute metrics using MSK time and exclude test agents
		try:
			msk = pytz.timezone("Europe/Moscow")
			today = datetime.now(msk).date()
			start_week = today - timedelta(days=today.weekday())
			start_month = today.replace(day=1)
			TEST_NAMES = {"agent1", "agent2", "agent3", "agent4"}
			res = self.db.client.table("employees").select("tg_id, agent_name, active, created_at").eq("active", True).execute()
			emps = [r for r in (getattr(res, "data", []) or []) if (str((r.get("agent_name") or "")).strip().lower() not in TEST_NAMES)]
			n_emps = max(1, len(emps))
			ids = [int(r["tg_id"]) for r in emps]
			def _utc_bounds(start: date, end: date) -> tuple[str, str]:
				start_dt = msk.localize(datetime.combine(start, datetime.min.time())).astimezone(pytz.UTC)
				end_dt = msk.localize(datetime.combine(end, datetime.max.time())).astimezone(pytz.UTC)
				return start_dt.isoformat(), end_dt.isoformat()
			def _count_logs(start: date, end: date, tg_id: int | None = None) -> int:
				lo, hi = _utc_bounds(start, end)
				q = self.db.client.table("logs").select("id").gte("created_at", lo).lte("created_at", hi)
				if tg_id is not None:
					q = q.eq("tg_id", tg_id)
				elif ids:
					q = q.in_("tg_id", ids)  # type: ignore
				res = q.execute(); return len(getattr(res, "data", []) or [])
			def _count_ai_messages(start: date, end: date, tg_id: int | None = None) -> int:
				lo, hi = _utc_bounds(start, end)
				q = self.db.client.table("assistant_messages").select("id").gte("created_at", lo).lte("created_at", hi).eq("role", "user")
				if tg_id is not None:
					q = q.eq("tg_id", tg_id)
				elif ids:
					q = q.in_("tg_id", ids)  # type: ignore
				res = q.execute(); return len(getattr(res, "data", []) or [])
			def _unique_users(start: date, end: date) -> int:
				lo, hi = _utc_bounds(start, end)
				q = self.db.client.table("logs").select("tg_id").gte("created_at", lo).lte("created_at", hi)
				if ids:
					q = q.in_("tg_id", ids)  # type: ignore
				res = q.execute(); s = {int(r["tg_id"]) for r in (getattr(res, "data", []) or []) if r.get("tg_id")}
				return len(s)
			periods = [("День", today, today), ("Неделя", start_week, today), ("Месяц", start_month, today)]
			rows_summary: List[Dict[str, object]] = []
			rows_users: List[Dict[str, object]] = []
			for label, start, end in periods:
				logs_total = _count_logs(start, end, None)
				ai_total = _count_ai_messages(start, end, None)
				uniq = _unique_users(start, end)
				rows_summary.append({
					"Период": label,
					"Логи всего": logs_total,
					"ИИ-запросов всего": ai_total,
					"Подключенных сотрудников": len(emps),
					"Частота логов на 1 сотрудника": round(logs_total / n_emps, 2),
					"Частота ИИ на 1 сотрудника": round(ai_total / n_emps, 2),
					"Уникальные пользователи": uniq,
					"Уникальные, % от всех": int(round((uniq * 100) / max(1, len(emps))))
				})
			for r in emps:
				uid = int(r["tg_id"]); name = r.get("agent_name") or f"agent?{uid}"
				rows_users.append({
					"Агент": name,
					"Логи (день)": _count_logs(today, today, uid),
					"Логи (неделя)": _count_logs(start_week, today, uid),
					"Логи (месяц)": _count_logs(start_month, today, uid),
					"ИИ (день)": _count_ai_messages(today, today, uid),
					"ИИ (неделя)": _count_ai_messages(start_week, today, uid),
					"ИИ (месяц)": _count_ai_messages(start_month, today, uid),
				})
			# Build XLSX
			buf = io.BytesIO(); filename = f"usability_{today.isoformat()}.xlsx"; ctype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
			if pd is not None:
				with pd.ExcelWriter(buf, engine="openpyxl") as writer:  # type: ignore
					pd.DataFrame(rows_summary).to_excel(writer, index=False, sheet_name="Сводка")
					pd.DataFrame(rows_users).to_excel(writer, index=False, sheet_name="По сотрудникам")
				buf.seek(0)
			else:
				import csv
				w = io.StringIO(); cw = csv.writer(w)
				cw.writerow(["Сводка"])
				if rows_summary:
					cw.writerow(list(rows_summary[0].keys()))
					for rr in rows_summary: cw.writerow(list(rr.values()))
				cw.writerow([]); cw.writerow(["По сотрудникам"])
				if rows_users:
					cw.writerow(list(rows_users[0].keys()))
					for rr in rows_users: cw.writerow(list(rr.values()))
				buf = io.BytesIO(w.getvalue().encode("utf-8-sig")); filename = filename.replace(".xlsx", ".csv"); ctype = "text/csv"
			# Send email
			msg = EmailMessage(); msg["From"] = settings.email_from; msg["To"] = settings.email_to_csv
			msg["Subject"] = f"AI BDM usability {today.isoformat()}"
			msg.set_content("Отчёт по юзабилити во вложении.")
			msg.add_attachment(buf.getvalue(), maintype=ctype.split('/')[0], subtype=ctype.split('/')[1], filename=filename)
			if getattr(settings, "smtp_ssl", False):
				s = smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port)
			else:
				s = smtplib.SMTP(settings.smtp_host, settings.smtp_port); s.starttls()
			s.login(settings.smtp_user, settings.smtp_pass); s.send_message(msg); s.quit()
			try:
				self.db.log(None, "email_report_sent", {"to": settings.email_to_csv, "filename": filename, "rows": len(rows_users)})
			except Exception:
				pass
		except Exception as e:
			try:
				self.db.log(None, "email_report_send_error", {"error": str(e)})
			except Exception:
				pass

	async def _autoclose_21_worker(self) -> None:
		"""Every minute: for each active employee, if local time is 21:00 and work is open, close and send end-of-day with STATS_JSON once per day."""
		try:
			emps = self.db.list_active_employees()
		except Exception:
			emps = []
		for r in (emps or []):
			# user id
			try:
				tg = int(r.get("tg_id"))
			except Exception:
				continue
			# only if session open
			if not self.db.work_is_open(tg):
				continue
			# local tz and time check
			tz_name = (r.get("timezone") or get_settings().timezone)
			try:
				tz = pytz.timezone(tz_name)
			except Exception:
				tz = pytz.timezone(get_settings().timezone)
			now_local = datetime.now(tz)
			# Trigger at 21:00 local time or later (in case the service missed exactly 21:00)
			if now_local.hour < 21:
				continue
			# dup guard: already closed today?
			try:
				rec = self.db.client.table("logs").select("created_at").eq("tg_id", tg).eq("action", "autoclose_21_sent").order("created_at", desc=True).limit(1).execute()
				rows = getattr(rec, "data", []) or []
				if rows:
					dt = datetime.fromisoformat(str(rows[0].get("created_at")).replace("Z", "+00:00")).astimezone(tz)
					if dt.date() == now_local.date():
						continue
			except Exception:
				pass
			# Build STATS and message
			today = now_local.date()
			start_week = today - timedelta(days=today.weekday())
			start_month = today.replace(day=1)
			name = r.get("agent_name") or f"agent?{tg}"
			# Totals and penetration
			today_total, today_by = self.db._sum_attempts_query(tg, today, today)
			week_total, _ = self.db._sum_attempts_query(tg, start_week, today)
			month_total, _ = self.db._sum_attempts_query(tg, start_month, today)
			m_day = self.db.meets_period_count(tg, today, today)
			m_week = self.db.meets_period_count(tg, start_week, today)
			m_month = self.db.meets_period_count(tg, start_month, today)
			linked_day = self.db.attempts_linked_period_count(tg, today, today)
			linked_week = self.db.attempts_linked_period_count(tg, start_week, today)
			linked_month = self.db.attempts_linked_period_count(tg, start_month, today)
			pen_day = (linked_day * 100 / m_day) if m_day > 0 else 0
			pen_week = (linked_week * 100 / m_week) if m_week > 0 else 0
			pen_month = (linked_month * 100 / m_month) if m_month > 0 else 0
			pen_target = int(self.db.compute_plan_breakdown(tg, today).get('penetration_target_pct', 50))
			# deltas (previous periods)
			start_prev_w = start_week - timedelta(days=7)
			end_prev_w = start_week - timedelta(days=1)
			m_prev_day = self.db.meets_period_count(tg, today - timedelta(days=1), today - timedelta(days=1))
			linked_prev_day = self.db.attempts_linked_period_count(tg, today - timedelta(days=1), today - timedelta(days=1))
			pen_prev_day = (linked_prev_day * 100 / m_prev_day) if m_prev_day > 0 else 0
			m_prev_week = self.db.meets_period_count(tg, start_prev_w, end_prev_w)
			linked_prev_week = self.db.attempts_linked_period_count(tg, start_prev_w, end_prev_w)
			pen_prev_week = (linked_prev_week * 100 / m_prev_week) if m_prev_week > 0 else 0
			end_prev_m = start_month - timedelta(days=1)
			start_prev_m = end_prev_m.replace(day=1)
			m_prev_month = self.db.meets_period_count(tg, start_prev_m, end_prev_m)
			linked_prev_month = self.db.attempts_linked_period_count(tg, start_prev_m, end_prev_m)
			pen_prev_month = (linked_prev_month * 100 / m_prev_month) if m_prev_month > 0 else 0
			def _delta_pp(a: float, b: float) -> int:
				return int(round(a - b))
			d_pen_day = _delta_pp(int(round(pen_day)), int(round(pen_prev_day)))
			d_pen_week = _delta_pp(int(round(pen_week)), int(round(pen_prev_week)))
			d_pen_month = _delta_pp(int(round(pen_month)), int(round(pen_prev_month)))
			# breakdown line
			items = [(p, c) for p, c in (today_by or {}).items() if c > 0]
			items.sort(key=lambda x: (-x[1], x[0]))
			breakdown = ", ".join([f"{c}{p}" for p, c in items]) if items else "—"
			lines: List[str] = []
			lines.append(f"- Сегодня: {int(round(pen_day))}% ({today_total}шт.) факт / {pen_target}% план / {int(round((pen_day/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_day}%")
			lines.append(f"- Сегодня по продуктам: — {breakdown}")
			lines.append(f"- Неделя: {int(round(pen_week))}% ({week_total}шт.) факт / {pen_target}% план / {int(round((pen_week/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_week}%")
			lines.append(f"- Месяц: {int(round(pen_month))}% ({month_total}шт.) факт / {pen_target}% план / {int(round((pen_month/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_month}%")
			# AI with STATS_JSON
			payload = {
				"period": {"label": "День", "start": today.isoformat(), "end": today.isoformat()},
				"current": {
					"day":   {"cross_fact": today_total,  "meet": m_day,   "penetration_pct": int(round(pen_day))},
					"week":  {"cross_fact": week_total,   "meet": m_week,  "penetration_pct": int(round(pen_week))},
					"month": {"cross_fact": month_total,  "meet": m_month, "penetration_pct": int(round(pen_month))},
				},
				"previous": {
					"day":   {"penetration_pct": int(round(pen_prev_day))},
					"week":  {"penetration_pct": int(round(pen_prev_week))},
					"month": {"penetration_pct": int(round(pen_prev_month))},
				},
				"targets": {"penetration_target_pct": pen_target}
			}
			policy = (
				"[AUTO_CLOSE_POLICY]\n"
				"Задача: оцени результаты из STATS_JSON и дай краткие рекомендации.\n"
				"Правила: используй только числа из STATS_JSON; сфокусируйся на повышении конверсии, не предлагай увеличивать количество встреч.\n"
				"Формат: 1) Выводы 2) Рекомендации/план на завтра.\n"
			)
			ai_prompt = policy + "\n[STATS_JSON]\n" + json.dumps(payload, ensure_ascii=False)
			stats_dwm = self.db.stats_day_week_month(tg, today)
			month_rank = self.db.month_ranking(start_month, today)
			comment = get_assistant_reply(self.db, tg, name, stats_dwm, month_rank, ai_prompt)
			final_msg = "\n".join(lines) + "\n" + self._shape_ai_comment(comment)
			# Close session first; retry once on failure, then send message; log outcome
			closed_ok = False
			try:
				self.db.work_close(tg)
				closed_ok = True
			except Exception as e:
				try:
					self.db.log(tg, "autoclose_21_close_error", {"error": str(e)[:200]})
				except Exception:
					pass
				# one more attempt
				try:
					self.db.work_close(tg)
					closed_ok = True
				except Exception:
					pass
			# Send message after attempting close
			await self.push_func(tg, final_msg)
			# Log autoclose outcome
			try:
				self.db.log(tg, "autoclose_21_sent", {"hour": 21, "closed": closed_ok})
			except Exception:
				pass 

	async def _email_report_fallback_worker(self) -> None:
		"""Every minute (MSK): if now past 11:00/19:00 and there is no email_report_sent today, send once and log fallback."""
		try:
			msk = pytz.timezone("Europe/Moscow")
			now = datetime.now(msk)
			if now.hour < 11:
				return
			# If after 11:00 but before 19:00, require at least one send today; if after 19:00 require second send
			res = self.db.client.table("logs").select("created_at").eq("action", "email_report_sent").order("created_at", desc=True).limit(5).execute()
			rows = getattr(res, "data", []) or []
			count_today = 0
			for r in rows:
				try:
					dt = datetime.fromisoformat(str(r.get("created_at")).replace("Z", "+00:00")).astimezone(msk)
					if dt.date() == now.date():
						count_today += 1
				except Exception:
					continue
			needed = 1 if now.hour < 19 else 2
			if count_today >= needed:
				return
			# send once
			await self._send_email_report()
			try:
				self.db.log(None, "email_report_fallback", {"needed": needed, "count_today": count_today})
			except Exception:
				pass
		except Exception:
			pass 

	async def _send_usability_report(self) -> None:
		settings = get_settings()
		if not settings.smtp_host or not settings.smtp_user or not settings.smtp_pass:
			try:
				self.db.log(None, "usab_report_skip", {"reason": "smtp_not_configured"})
			except Exception:
				pass
			return
		# recipients fixed as requested
		recipients = ["sergey.tokarev@domrf.ru", "flagk@mail.ru"]
		# compute metrics for day/week/month
		try:
			msk = pytz.timezone("Europe/Moscow")
			today = datetime.now(msk).date()
			start_week = today - timedelta(days=today.weekday())
			start_month = today.replace(day=1)
			# Active employees excluding test names
			TEST_NAMES = {"agent1", "agent2", "agent3", "agent4"}
			res = self.db.client.table("employees").select("tg_id, agent_name, active").eq("active", True).execute()
			emps = [r for r in (getattr(res, "data", []) or []) if (str((r.get("agent_name") or "")).strip().lower() not in TEST_NAMES)]
			n_emps = max(1, len(emps))
			ids = [int(r["tg_id"]) for r in emps]
			# Helper: MSK -> UTC period bounds
			def _utc_bounds(start: date, end: date) -> tuple[str, str]:
				start_dt = msk.localize(datetime.combine(start, datetime.min.time())).astimezone(pytz.UTC)
				end_dt = msk.localize(datetime.combine(end, datetime.max.time())).astimezone(pytz.UTC)
				return start_dt.isoformat(), end_dt.isoformat()
			# Helper: count logs by period and per user (only active non-test employees)
			def _count_logs(start: date, end: date, tg_id: int | None = None) -> int:
				lo, hi = _utc_bounds(start, end)
				q = self.db.client.table("logs").select("id").gte("created_at", lo).lte("created_at", hi)
				if tg_id is not None:
					q = q.eq("tg_id", tg_id)
				elif ids:
					q = q.in_("tg_id", ids)  # type: ignore
				res = q.execute(); return len(getattr(res, "data", []) or [])
			def _count_ai_messages(start: date, end: date, tg_id: int | None = None) -> int:
				lo, hi = _utc_bounds(start, end)
				q = self.db.client.table("assistant_messages").select("id").gte("created_at", lo).lte("created_at", hi).eq("role", "user")
				if tg_id is not None:
					q = q.eq("tg_id", tg_id)
				elif ids:
					q = q.in_("tg_id", ids)  # type: ignore
				res = q.execute(); return len(getattr(res, "data", []) or [])
			def _unique_users(start: date, end: date) -> int:
				lo, hi = _utc_bounds(start, end)
				q = self.db.client.table("logs").select("tg_id").gte("created_at", lo).lte("created_at", hi)
				if ids:
					q = q.in_("tg_id", ids)  # type: ignore
				res = q.execute(); s = {int(r["tg_id"]) for r in (getattr(res, "data", []) or []) if r.get("tg_id")}
				return len(s)
			# Totals and per-user counts
			periods = [("День", today, today), ("Неделя", start_week, today), ("Месяц", start_month, today)]
			rows_summary: List[Dict[str, object]] = []
			rows_users: List[Dict[str, object]] = []
			for label, start, end in periods:
				logs_total = _count_logs(start, end, None)
				ai_total = _count_ai_messages(start, end, None)
				uniq = _unique_users(start, end)
				rows_summary.append({
					"Период": label,
					"Логи всего": logs_total,
					"ИИ-запросов всего": ai_total,
					"Подключенных сотрудников": len(emps),
					"Частота логов на 1 сотрудника": round(logs_total / n_emps, 2),
					"Частота ИИ на 1 сотрудника": round(ai_total / n_emps, 2),
					"Уникальные пользователи": uniq,
					"Уникальные, % от всех": int(round((uniq * 100) / max(1, len(emps))))
				})
			# per-user table (day/week/month columns)
			for r in emps:
				uid = int(r["tg_id"]); name = r.get("agent_name") or f"agent?{uid}"
				rows_users.append({
					"Агент": name,
					"Логи (день)": _count_logs(today, today, uid),
					"Логи (неделя)": _count_logs(start_week, today, uid),
					"Логи (месяц)": _count_logs(start_month, today, uid),
					"ИИ (день)": _count_ai_messages(today, today, uid),
					"ИИ (неделя)": _count_ai_messages(start_week, today, uid),
					"ИИ (месяц)": _count_ai_messages(start_month, today, uid),
				})
			# Build daily usage pivot since FIRST non-test employee connect date (any status)
			first_date = today
			ids_all: List[int] = []
			emp_all_map: Dict[int, str] = {}
			try:
				res_all = self.db.client.table("employees").select("tg_id, agent_name, created_at").execute()
				all_rows = (getattr(res_all, "data", []) or [])
				for rr in all_rows:
					name = (rr.get("agent_name") or "").strip().lower()
					if name in TEST_NAMES:
						continue
					try:
						tid = int(rr.get("tg_id"))
						emp_all_map[tid] = rr.get("agent_name") or f"agent?{tid}"
						ids_all.append(tid)
					except Exception:
						continue
					created = rr.get("created_at")
					if created:
						dt = datetime.fromisoformat(str(created).replace("Z", "+00:00")).astimezone(msk)
						fd = dt.date()
						if fd < first_date:
							first_date = fd
			except Exception:
				pass
			# Also check earliest log across non-test employees
			try:
				if ids_all:
					q_first = (
						self.db.client.table("logs")
						.select("created_at, tg_id")
						.in_("tg_id", ids_all)  # type: ignore
						.order("created_at")
						.limit(1)
					)
					res_first = q_first.execute()
					rows_first = (getattr(res_first, "data", []) or [])
					if rows_first:
						dt0 = datetime.fromisoformat(str(rows_first[0].get("created_at")).replace("Z", "+00:00")).astimezone(msk)
						if dt0.date() < first_date:
							first_date = dt0.date()
			except Exception:
				pass
			if first_date > today:
				first_date = today
			lo, hi = _utc_bounds(first_date, today)
			# Limit logs to non-test employees if possible
			if ids_all:
				logs_res = (
					self.db.client.table("logs")
					.select("tg_id, created_at")
					.in_("tg_id", ids_all)  # type: ignore
					.gte("created_at", lo)
					.lte("created_at", hi)
					.execute()
				)
			else:
				logs_res = self.db.client.table("logs").select("tg_id, created_at").gte("created_at", lo).lte("created_at", hi).execute()
			# For daily sheet, use all connected employees (non-test), not only active
			emp_map = emp_all_map if emp_all_map else {int(r["tg_id"]): (r.get("agent_name") or f"agent?{r['tg_id']}") for r in emps}
			daily_counts: Dict[date, Dict[int, int]] = {}
			for row in (getattr(logs_res, "data", []) or []):
				try:
					tg = int(row.get("tg_id")) if row.get("tg_id") is not None else None
					if (tg is None) or ((ids_all and tg not in ids_all)):
						continue
					cdt = datetime.fromisoformat(str(row.get("created_at")).replace("Z", "+00:00")).astimezone(msk)
					cd = cdt.date()
					d = daily_counts.get(cd)
					if not d:
						d = {}
						daily_counts[cd] = d
					d[tg] = d.get(tg, 0) + 1
				except Exception:
					continue
			rows_days: List[Dict[str, object]] = []
			cur = first_date
			# ordered list of employees (by name) for stable columns
			ordered = sorted([(tid, emp_map[tid]) for tid in (ids_all if ids_all else ids)], key=lambda x: x[1])
			while cur <= today:
				row: Dict[str, object] = {"Дата": cur.isoformat()}
				per = daily_counts.get(cur, {})
				for tid, name in ordered:
					row[name] = int(per.get(tid, 0))
				rows_days.append(row)
				cur += timedelta(days=1)
			# Build Excel with three sheets
			buf = io.BytesIO()
			filename = f"usability_{today.isoformat()}.xlsx"
			ctype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
			if pd is not None:
				with pd.ExcelWriter(buf, engine="openpyxl") as writer:  # type: ignore
					pd.DataFrame(rows_summary).to_excel(writer, index=False, sheet_name="Сводка")
					pd.DataFrame(rows_users).to_excel(writer, index=False, sheet_name="По сотрудникам")
					pd.DataFrame(rows_days).to_excel(writer, index=False, sheet_name="По дням")
				buf.seek(0)
			else:
				# CSV fallback: single sheet equivalent by concatenation
				import csv
				w = io.StringIO()
				cw = csv.writer(w)
				cw.writerow(["Сводка"])
				if rows_summary:
					cw.writerow(list(rows_summary[0].keys()))
					for rr in rows_summary:
						cw.writerow(list(rr.values()))
				cw.writerow([]); cw.writerow(["По сотрудникам"])
				if rows_users:
					cw.writerow(list(rows_users[0].keys()))
					for rr in rows_users:
						cw.writerow(list(rr.values()))
				cw.writerow([]); cw.writerow(["По дням"])
				if rows_days:
					cw.writerow(list(rows_days[0].keys()))
					for rr in rows_days:
						cw.writerow(list(rr.values()))
				buf = io.BytesIO(w.getvalue().encode("utf-8-sig")); filename = filename.replace(".xlsx", ".csv"); ctype = "text/csv"
			# Send email
			msg = EmailMessage(); msg["From"] = settings.email_from; msg["To"] = ", ".join(recipients)
			msg["Subject"] = f"AI BDM usability {today.isoformat()}"
			msg.set_content("Отчёт по юзабилити во вложении.")
			msg.add_attachment(buf.getvalue(), maintype=ctype.split('/')[0], subtype=ctype.split('/')[1], filename=filename)
			if getattr(settings, "smtp_ssl", False):
				s = smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port)
			else:
				s = smtplib.SMTP(settings.smtp_host, settings.smtp_port); s.starttls()
			s.login(settings.smtp_user, settings.smtp_pass); s.send_message(msg); s.quit()
			try:
				self.db.log(None, "usab_report_sent", {"to": recipients, "filename": filename, "emps": len(emps)})
			except Exception:
				pass
		except Exception as e:
			try:
				self.db.log(None, "usab_report_error", {"error": str(e)})
			except Exception:
				pass
