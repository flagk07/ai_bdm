from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Callable, Dict, Tuple

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from .config import get_settings
from .db import Database


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
			# current totals
			today_total = self.db._sum_attempts_query(tg, today, today)[0]
			week_total = self.db._sum_attempts_query(tg, start_week, end_week)[0]
			month_total = self.db._sum_attempts_query(tg, start_month, end_month)[0]
			# previous totals
			prev_day_total = self.db._sum_attempts_query(tg, today - timedelta(days=1), today - timedelta(days=1))[0]
			prev_week_total = self.db._sum_attempts_query(tg, start_prev_w, end_prev_w)[0]
			prev_month_total = self.db._sum_attempts_query(tg, start_prev_m, end_prev_m)[0]
			# deltas
			d_day = self._delta_pct(today_total, prev_day_total)
			d_week = self._delta_pct(week_total, prev_week_total)
			d_month = self._delta_pct(month_total, prev_month_total)
			# simple recommendations
			recs = []
			if d_day < 0:
				recs.append("усилить сегодня 1‑2 продукта")
			if d_week < 0:
				recs.append("закрыть просадку по неделе")
			if d_month < 0:
				recs.append("пересобрать план на месяц")
			if not recs:
				recs.append("держим темп")
			rec_line = "; ".join(recs)
			# goals: placeholder (не задана)
			goal_line = "—"
			text = (
				f"1. {name} — авто‑сводка\n"
				f"2. Сегодня: {today_total} (Δ {d_day}%) 🎯\n"
				f"3. Неделя: {week_total} (Δ {d_week}%) 📅\n"
				f"4. Месяц: {month_total} (Δ {d_month}%) 📊\n"
				f"5. Цели: {goal_line} 🎯\n"
				f"6. Комментарий: {rec_line} 💡\n"
				f"7. Продолжить: /assistant"
			)
			await self.push_func(tg, text)

	def start(self) -> None:
		# Daily at 20:00 local time
		self.scheduler.add_job(self._send_daily, CronTrigger(hour=20, minute=0))
		# Every 5 minutes periodic summary (test mode)
		self.scheduler.add_job(self._send_periodic, CronTrigger(minute="*/5"))
		self.scheduler.start() 