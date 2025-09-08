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
			# restrict autosummaries to tester only
			try:
				if int(r["tg_id"]) != 195830791:
					continue
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
			# restrict autosummaries to tester only
			try:
				tg = int(r["tg_id"])
				if tg != 195830791:
					continue
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
			# message header and lines with "- " and [F#] citations
			header = f"{name} — авто‑сводка\n"
			lines = []
			# day line
			if show_d_day:
				lines.append(f"- Сегодня: {today_total} факт / {p_day} план / {self._fmt1(c_day)}% выполнение / {self._fmt1(pen_day)}% проникновение / Δ {self._format_delta(d_day)}%")
			else:
				lines.append(f"- Сегодня: {today_total} факт / {p_day} план / {self._fmt1(c_day)}% выполнение / {self._fmt1(pen_day)}% проникновение")
			# products
			lines.append(f"- Сегодня по продуктам: {breakdown}")
			# week line
			if show_d_week:
				lines.append(f"- Неделя: {week_total} факт / {p_week} план / {self._fmt1(c_week)}% выполнение / {self._fmt1(pen_week)}% проникновение / Δ {self._format_delta(d_week)}%")
			else:
				lines.append(f"- Неделя: {week_total} факт / {p_week} план / {self._fmt1(c_week)}% выполнение / {self._fmt1(pen_week)}% проникновение")
			# month line
			if show_d_month:
				lines.append(f"- Месяц: {month_total} факт / {p_month} план / {self._fmt1(c_month)}% выполнение / {self._fmt1(pen_month)}% проникновение / Δ {self._format_delta(d_month)}%")
			else:
				lines.append(f"- Месяц: {month_total} факт / {p_month} план / {self._fmt1(c_month)}% выполнение / {self._fmt1(pen_month)}% проникновение")
			# RR month
			lines.append(f"- RR месяца: прогноз факта {rr} / {rr_pct}% прогноз выполнения")
			text = header + "\n".join(lines) + "\n"
			# AUTO_SUMMARY: build policy + STATS_JSON and get AI conclusions
			# Targets to reach monthly plan using remaining working days of the month
			last_day_month = (start_month.replace(day=28) + timedelta(days=10)).replace(day=1) - timedelta(days=1)
			remain_wd = max(count_workdays(today + timedelta(days=1), last_day_month), 0)
			gap_total = max(p_month - month_total, 0)
			need_per_day = int((gap_total + max(remain_wd, 1) - 1) // max(remain_wd, 1)) if remain_wd > 0 else gap_total
			need_per_week = need_per_day * 5
			payload = {
				"period": {"label": "Месяц", "start": start_month.isoformat(), "end": today.isoformat()},
				"current": {
					"day":   {"fact": today_total,  "plan": p_day,   "done_pct": int(round(c_day)),   "meet": m_day,   "penetration_pct": int(round(pen_day))},
					"week":  {"fact": week_total,  "plan": p_week,  "done_pct": int(round(c_week)),  "meet": m_week,  "penetration_pct": int(round(pen_week))},
					"month": {"fact": month_total, "plan": p_month, "done_pct": int(round(c_month)), "meet": m_month, "penetration_pct": int(round(pen_month)), "rr": rr, "rr_pct": rr_pct},
				},
				"previous": {
					"day":   {"fact": prev_day_total},
					"week":  {"fact": prev_week_total},
					"month": {"fact": prev_month_total},
				},
				"targets": {
					"gap_total": gap_total,
					"remaining_workdays": remain_wd,
					"need_per_day": need_per_day,
					"need_per_week": need_per_week
				}
			}
			policy = (
				"[AUTO_SUMMARY_POLICY]\n"
				"Задача\n- Оценка результатов из STATS_JSON\n- Формирование выводов и рекомендаций на основании оцененных результатов\n\n"
				"Правила\n- Числа разрешено брать ТОЛЬКО из STATS_JSON. Ничего не придумывай и не изменяй\n- Остальные правила из системного промпта\n\n"
				"Формат ответа\n1. Выводы по текущим результатам\n<сгенерированный ответ выводов о работе на основании результатов>\n"
				"2. Рекомендации и цели\n<сгенерированный ответ с рекомендациями и целями по повышению эффективности работы на основании результатов и выводов. Укажи конкретику из targets: нужно need_per_day в день (≈ need_per_week в неделю) до конца месяца>\n"
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
		# Daily at 20:00 local time
		self.scheduler.add_job(self._send_daily, CronTrigger(hour=20, minute=0))
		# Every 5 minutes periodic summary (test mode)
		self.scheduler.add_job(self._send_periodic, CronTrigger(minute="*/5"))
		self.scheduler.start() 