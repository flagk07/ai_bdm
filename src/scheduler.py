from __future__ import annotations

from datetime import date, datetime
from typing import Callable

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

	def start(self) -> None:
		# Daily at 20:00 local time
		self.scheduler.add_job(self._send_daily, CronTrigger(hour=20, minute=0))
		self.scheduler.start() 