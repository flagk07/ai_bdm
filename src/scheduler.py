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

	async def _send_daily(self) -> None:
		today = date.today()
		# get all employees
		emps = self.db.client.table("employees").select("tg_id, agent_name, active").eq("active", True).execute()
		for r in (emps.data or []):
			try:
				int(r["tg_id"])  # validate tg_id
			except Exception:
				continue
			stats = self.db.stats_day_week_month(int(r["tg_id"]), today)
			text = (
				f"{r['agent_name']}: сегодня {stats['today']['total']}, неделя {stats['week']['total']}, месяц {stats['month']['total']}"
			)
			await self.push_func(int(r["tg_id"]), text)

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

	async def _send_periodic(self) -> None:
		today = date.today()
		start_week, end_week = self._week_range(today)
		start_prev_w, end_prev_w = self._prev_week_range(today)
		start_month, end_month = self._month_range(today)
		start_prev_m, end_prev_m = self._prev_month_range(today)

		emps = self.db.client.table("employees").select("tg_id, agent_name, active, created_at").eq("active", True).execute()
		for r in (emps.data or []):
			try:
				tg = int(r["tg_id"])  # validate
			except Exception:
				continue
			name = r["agent_name"]
			# parse employee registration date
			created_at_raw = r.get("created_at")
			created_at_date: date | None = None
			try:
				if created_at_raw:
					created_at_date = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00")).date()
			except Exception:
				created_at_date = None
			# current totals and breakdown for today
			today_total, today_by = self.db._sum_attempts_query(tg, today, today)
			# Guard: ensure today_total equals sum of breakdown
			sum_by = sum((today_by or {}).values())
			if sum_by != today_total:
				try:
					self.db.log(tg, "today_total_mismatch", {"expected_total": today_total, "sum_by": sum_by, "by": today_by})
				except Exception:
					pass
				today_total = sum_by
			week_total, _ = self.db._sum_attempts_query(tg, start_week, end_week)
			month_total, _ = self.db._sum_attempts_query(tg, start_month, end_month)
			# previous totals
			prev_day_total, _ = self.db._sum_attempts_query(tg, today - timedelta(days=1), today - timedelta(days=1))
			prev_week_total, _ = self.db._sum_attempts_query(tg, start_prev_w, end_prev_w)
			prev_month_total, _ = self.db._sum_attempts_query(tg, start_prev_m, end_prev_m)
			# deltas
			d_day = self._delta_pct(today_total, prev_day_total)
			d_week = self._delta_pct(week_total, prev_week_total)
			d_month = self._delta_pct(month_total, prev_month_total)
			# gating: show delta only if employee existed in the whole previous period
			show_d_day = False
			show_d_week = False
			show_d_month = False
			if created_at_date:
				prev_day = today - timedelta(days=1)
				show_d_day = created_at_date <= prev_day
				show_d_week = created_at_date <= end_prev_w
				show_d_month = created_at_date <= end_prev_m
			# plans and RR
			plan = self.db.compute_plan_breakdown(tg, today)
			rr = int(plan.get('rr_month', 0))
			plan_m = int(plan.get('plan_month', 0))
			rr_pct = int(round(rr * 100 / plan_m)) if plan_m > 0 else 0
			# completion percents (one decimal)
			p_day = int(plan.get('plan_day', 0))
			p_week = int(plan.get('plan_week', 0))
			p_month = int(plan.get('plan_month', 0))
			c_day = (today_total * 100 / p_day) if p_day > 0 else 0
			c_week = (week_total * 100 / p_week) if p_week > 0 else 0
			c_month = (month_total * 100 / p_month) if p_month > 0 else 0
			# meetings and penetration
			m_day = self.db.meets_period_count(tg, today, today)
			m_week = self.db.meets_period_count(tg, start_week, end_week)
			m_month = self.db.meets_period_count(tg, start_month, end_month)
			linked_day = self.db.attempts_linked_period_count(tg, today, today)
			linked_week = self.db.attempts_linked_period_count(tg, start_week, end_week)
			linked_month = self.db.attempts_linked_period_count(tg, start_month, end_month)
			pen_day = (linked_day * 100 / m_day) if m_day > 0 else 0
			pen_week = (linked_week * 100 / m_week) if m_week > 0 else 0
			pen_month = (linked_month * 100 / m_month) if m_month > 0 else 0
			# format breakdown like "2КН, 3КСП"
			items = [(p, c) for p, c in (today_by or {}).items() if c > 0]
			items.sort(key=lambda x: (-x[1], x[0]))
			breakdown = ", ".join([f"{c}{p}" for p, c in items]) if items else "—"
			# build FACTS map for [F#] citations
			# internal facts retained for prompt only; не выводим пользователю
			facts_lines: List[str] = []
			# message header and lines with penetration-focused metrics
			header = f"{name} — авто‑сводка\n"
			# previous period penetrations for deltas
			m_prev_day = self.db.meets_period_count(tg, today - timedelta(days=1), today - timedelta(days=1))
			linked_prev_day = self.db.attempts_linked_period_count(tg, today - timedelta(days=1), today - timedelta(days=1))
			pen_prev_day = (linked_prev_day * 100 / m_prev_day) if m_prev_day > 0 else 0
			m_prev_week = self.db.meets_period_count(tg, start_prev_w, end_prev_w)
			linked_prev_week = self.db.attempts_linked_period_count(tg, start_prev_w, end_prev_w)
			pen_prev_week = (linked_prev_week * 100 / m_prev_week) if m_prev_week > 0 else 0
			m_prev_month = self.db.meets_period_count(tg, start_prev_m, end_prev_m)
			linked_prev_month = self.db.attempts_linked_period_count(tg, start_prev_m, end_prev_m)
			pen_prev_month = (linked_prev_month * 100 / m_prev_month) if m_prev_month > 0 else 0
			# completion vs plan (Variant A): (fact_pen / plan_pen) * 100
			pen_target = int(self.db.compute_plan_breakdown(tg, today).get('penetration_target_pct', 50))
			comp_day = int(round((pen_day / (pen_target if pen_target > 0 else 1)) * 100))
			comp_week = int(round((pen_week / (pen_target if pen_target > 0 else 1)) * 100))
			comp_month = int(round((pen_month / (pen_target if pen_target > 0 else 1)) * 100))
			# deltas by penetration percent
			d_pen_day = self._delta_pct(int(round(pen_day)), int(round(pen_prev_day)))
			d_pen_week = self._delta_pct(int(round(pen_week)), int(round(pen_prev_week)))
			d_pen_month = self._delta_pct(int(round(pen_month)), int(round(pen_prev_month)))
			lines = []
			lines.append(f"- Сегодня: {self._fmt1(pen_day)}% ({today_total}шт.) факт / {pen_target}% план / {self._fmt1(comp_day)}% выполнение / Δ {self._format_delta(d_pen_day)}%")
			lines.append(f"- Сегодня по продуктам: {breakdown}")
			lines.append(f"- Неделя: {self._fmt1(pen_week)}% ({week_total}шт.) факт / {pen_target}% план / {self._fmt1(comp_week)}% выполнение / Δ {self._format_delta(d_pen_week)}%")
			lines.append(f"- Месяц: {self._fmt1(pen_month)}% ({month_total}шт.) факт / {pen_target}% план / {self._fmt1(comp_month)}% выполнение / Δ {self._format_delta(d_pen_month)}%")
			text = header + "\n".join(lines) + "\n"
			# AUTO_SUMMARY: build policy + STATS_JSON and get AI conclusions (penetration target)
			pen_target = int(self.db.compute_plan_breakdown(tg, today).get('penetration_target_pct', 50))
			# current penetration
			pen_day = (linked_day * 100 / m_day) if m_day > 0 else 0
			pen_week = (linked_week * 100 / m_week) if m_week > 0 else 0
			pen_month = (linked_month * 100 / m_month) if m_month > 0 else 0
			# For targets, we do not propose increasing meetings count; focus on conversion
			gap_pen_month = max(pen_target - int(round(pen_month)), 0)
			gap_pen_week = max(pen_target - int(round(pen_week)), 0)
			payload = {
				"period": {"label": "Месяц", "start": start_month.isoformat(), "end": today.isoformat()},
				"current": {
					"day":   {"cross_fact": today_total,  "meet": m_day,   "penetration_pct": int(round(pen_day))},
					"week":  {"cross_fact": week_total,  "meet": m_week,  "penetration_pct": int(round(pen_week))},
					"month": {"cross_fact": month_total, "meet": m_month, "penetration_pct": int(round(pen_month))},
				},
				"previous": {
					"day":   {"fact": prev_day_total},
					"week":  {"fact": prev_week_total},
					"month": {"fact": prev_month_total},
				},
				"targets": {
					"penetration_target_pct": pen_target,
					"gap_penetration_month_pct": gap_pen_month,
					"gap_penetration_week_pct": gap_pen_week
				}
			}
			policy = (
				"[AUTO_SUMMARY_POLICY]\n"
				"Задача\n- Оценка результатов из STATS_JSON\n- Формирование выводов и рекомендаций на основании оцененных результатов\n\n"
				"Правила\n- Числа разрешено брать ТОЛЬКО из STATS_JSON. Ничего не придумывай и не изменяй\n- Остальные правила из системного промпта\n\n"
				"Формат ответа\n1. Выводы по текущим результатам\n<сгенерированный ответ выводов о работе на основании результатов>\n"
				"2. Рекомендации и цели\n<сфокусируйся на повышении конверсии кросс-продаж (проникновение), не предлагай увеличивать количество встреч. Ориентируйся на цель penetration_target_pct и текущую penetration_pct.>\n"
			)
			ai_prompt = (
				policy + "\n[STATS_JSON]\n" + json.dumps(payload, ensure_ascii=False)
			)
			stats_dwm = self.db.stats_day_week_month(tg, today)
			month_rank = self.db.month_ranking(start_month, end_month)
			comment = get_assistant_reply(self.db, tg, name, stats_dwm, month_rank, ai_prompt)
			text += self._shape_ai_comment(comment) + "\n"
			# Store auto-sent summary in assistant_messages (auto=true)
			try:
				self.db.add_assistant_message(tg, "assistant", text, off_topic=False, auto=True)
			except Exception:
				pass
			await self.push_func(tg, text)

	def start(self) -> None:
		# Flags
		notify_enabled = os.environ.get("NOTIFY_ENABLED", "1").lower() not in ("0","false","no","off")
		email_enabled = os.environ.get("EMAIL_REPORT_ENABLED", "1").lower() not in ("0","false","no","off")
		# Daily advisor summary at 20:00
		if notify_enabled:
			self.scheduler.add_job(self._send_daily, CronTrigger(hour=20, minute=0))
		# Periodic summary — only when notifications enabled
		if notify_enabled:
			self.scheduler.add_job(self._send_periodic, CronTrigger(minute="*/120"))
		# Email report at 11:00 and 19:00 Moscow time
		if email_enabled:
			self.scheduler.add_job(self._send_email_report, CronTrigger(hour="11,19", minute=0))
		self.scheduler.start()

	async def _send_email_report(self) -> None:
		settings = get_settings()
		if not settings.smtp_host or not settings.smtp_user or not settings.smtp_pass:
			try:
				self.db.log(None, "email_report_skip", {"reason": "smtp_not_configured"})
			except Exception:
				pass
			return
		# ensure recipient is set
		if not getattr(settings, "email_to_csv", ""):
			try:
				self.db.log(None, "email_report_skip", {"reason": "recipient_not_set"})
			except Exception:
				pass
			return
		# gather data
		day = date.today()
		start_week = day - timedelta(days=day.weekday())
		start_month = day.replace(day=1)
		rows: List[Dict[str, object]] = []
		rec = self.db.client.table("employees").select("tg_id, agent_name, active").eq("active", True).execute()
		for r in (getattr(rec, "data", []) or []):
			tg = int(r["tg_id"]); name = r.get("agent_name") or f"agent?{tg}"
			# meets
			m_day = self.db.meets_period_count(tg, day, day)
			m_month = self.db.meets_period_count(tg, start_month, day)
			# cross attempts
			t_day, _ = self.db._sum_attempts_query(tg, day, day)
			t_month, _ = self.db._sum_attempts_query(tg, start_month, day)
			# penetration and completion (vs target)
			pen_day = (t_day * 100 / m_day) if m_day > 0 else 0
			pen_month = (t_month * 100 / m_month) if m_month > 0 else 0
			target = int(self.db.compute_plan_breakdown(tg, day).get("penetration_target_pct", 50))
			compl_day = int(round((pen_day / (target if target > 0 else 1)) * 100))
			compl_month = int(round((pen_month / (target if target > 0 else 1)) * 100))
			rows.append({
				"tg_id": tg,
				"Агент": name,
				"Встречи (день)": m_day,
				"Встречи (месяц)": m_month,
				"Кросс (день)": t_day,
				"Кросс (месяц)": t_month,
				"Проникновение % (день)": int(round(pen_day)),
				"Проникновение % (месяц)": int(round(pen_month)),
				"Выполнение % (день)": compl_day,
				"Выполнение % (месяц)": compl_month,
			})
		# build XLSX bytes
		buf = io.BytesIO()
		try:
			if pd is not None:
				df = pd.DataFrame(rows)
				df.to_excel(buf, index=False)
				filename = f"report_{day.isoformat()}.xlsx"
				ctype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
			else:
				# fallback to CSV
				import csv
				filename = f"report_{day.isoformat()}.csv"
				ctype = "text/csv"
				w = io.StringIO()
				writer = csv.DictWriter(w, fieldnames=list(rows[0].keys()) if rows else [])
				writer.writeheader()
				for rr in rows: writer.writerow(rr)
				buf = io.BytesIO(w.getvalue().encode("utf-8-sig"))
		except Exception as e:
			try:
				self.db.log(None, "email_report_build_error", {"error": str(e)})
			except Exception:
				pass
			return
		# send email
		try:
			msg = EmailMessage()
			msg["From"] = settings.email_from
			msg["To"] = settings.email_to_csv
			msg["Subject"] = f"AI BDM отчёт {day.isoformat()}"
			msg.set_content("Автоматический отчёт во вложении.")
			msg.add_attachment(buf.getvalue(), maintype=ctype.split('/')[0], subtype=ctype.split('/')[1], filename=filename)
			if getattr(settings, "smtp_ssl", False):
				s = smtplib.SMTP_SSL(settings.smtp_host, settings.smtp_port)
			else:
				s = smtplib.SMTP(settings.smtp_host, settings.smtp_port)
				s.starttls()
			s.login(settings.smtp_user, settings.smtp_pass)
			s.send_message(msg)
			s.quit()
			try:
				self.db.log(None, "email_report_sent", {"to": settings.email_to_csv, "filename": filename, "rows": len(rows)})
			except Exception:
				pass
		except Exception as e:
			try:
				self.db.log(None, "email_report_send_error", {"error": str(e)})
			except Exception:
				pass 
