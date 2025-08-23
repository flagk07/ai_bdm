from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Callable, Dict, Tuple

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from .config import get_settings
from .db import Database
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

	async def _send_daily(self) -> None:
		today = date.today()
		# get all employees
		emps = self.db.client.table("employees").select("tg_id, agent_name, active").eq("active", True).execute()
		for r in (emps.data or []):
			stats = self.db.stats_day_week_month(int(r["tg_id"]), today)
			text = (
				f"{r['agent_name']}: сегодня {stats['today']['total']}, неделя {stats['week']['total']}, месяц {stats['month']['total']}"
			)
			await self.push_func(int(r["tg_id"]), text)

	async def _send_periodic(self) -> None:
		today = date.today()
		start_week, end_week = self._week_range(today)
		start_prev_w, end_prev_w = self._prev_week_range(today)
		start_month, end_month = self._month_range(today)
		start_prev_m, end_prev_m = self._prev_month_range(today)

		emps = self.db.client.table("employees").select("tg_id, agent_name, active").eq("active", True).execute()
		for r in (emps.data or []):
			tg = int(r["tg_id"])
			name = r["agent_name"]
			# current totals and breakdown for today
			today_total, today_by = self.db._sum_attempts_query(tg, today, today)
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
			# format breakdown like "2КН, 3КСП"
			items = [(p, c) for p, c in (today_by or {}).items() if c > 0]
			items.sort(key=lambda x: (-x[1], x[0]))
			breakdown = ", ".join([f"{c}{p}" for p, c in items]) if items else "—"
			# message header and lines with "- "
			header = f"{name} — авто‑сводка\n"
			lines = [
				f"- Сегодня: {today_total} (Δ {d_day}%) 🎯",
				f"- Сегодня по продуктам: {breakdown}",
				f"- Неделя: {week_total} (Δ {d_week}%) 📅",
				f"- Месяц: {month_total} (Δ {d_month}%) 📊",
			]
			# assistant live plain-text comment
			stats_dwm = self.db.stats_day_week_month(tg, today)
			month_rank = self.db.month_ranking(start_month, end_month)
			assistant_prompt = (
				"Дай краткий комментарий по динамике за сегодня/неделю/месяц в виде одного абзаца "
				"(2–3 коротких предложения), без списков и нумерации, без жирного и эмодзи. "
				"Если есть снижение — задай 1 уточняющий вопрос и предложи 1 конкретное действие. "
				"Учитывай, что после сообщения сотрудник может перейти в /assistant. "
				f"Данные: сегодня {today_total} (Δ {d_day}%), неделя {week_total} (Δ {d_week}%), месяц {month_total} (Δ {d_month}%)."
			)
			assistant_comment = get_assistant_reply(self.db, tg, name, stats_dwm, month_rank, assistant_prompt)
			assistant_comment = assistant_comment.replace('\n', ' ').strip()
			# final text
			text = (
				header + "\n".join(lines) + "\n"
				+ f"Комментарий ассистента: {assistant_comment}\n"
				+ "Продолжить: /assistant"
			)
			await self.push_func(tg, text)

	def start(self) -> None:
		# Daily at 20:00 local time
		self.scheduler.add_job(self._send_daily, CronTrigger(hour=20, minute=0))
		# Every 5 minutes periodic summary (test mode)
		self.scheduler.add_job(self._send_periodic, CronTrigger(minute="*/5"))
		self.scheduler.start() 