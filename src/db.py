from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import create_client, Client

from .config import get_settings
from .pii import sanitize_text


# Simple RU 2025 workdays calendar: Mon-Fri are working days; exclude official holidays
_RU_2025_HOLIDAYS = {
	date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 6), date(2025, 1, 7), date(2025, 1, 8),
	date(2025, 2, 24),
	date(2025, 3, 10),
	date(2025, 5, 1), date(2025, 5, 2), date(2025, 5, 9),
	date(2025, 6, 12),
	date(2025, 11, 4),
}


def is_workday(d: date) -> bool:
	# Mon-Fri and not holiday
	return d.weekday() < 5 and d not in _RU_2025_HOLIDAYS


def count_workdays(start: date, end: date) -> int:
	cnt = 0
	d = start
	while d <= end:
		if is_workday(d):
			cnt += 1
		d += timedelta(days=1)
	return cnt


def month_workdays(d: date) -> int:
	first = d.replace(day=1)
	last = (first.replace(day=28) + timedelta(days=10)).replace(day=1) - timedelta(days=1)
	return count_workdays(first, last)


def month_workdays_elapsed(d: date) -> int:
	first = d.replace(day=1)
	return count_workdays(first, d)


@dataclass
class Employee:
	tg_id: int
	agent_name: str
	active: bool


class Database:
	def __init__(self) -> None:
		settings = get_settings()
		self.client: Client = create_client(settings.supabase_url, settings.supabase_api_key)

	def ensure_allowed_users_bootstrap(self, allowed_ids: List[int]) -> None:
		if not allowed_ids:
			return
		for tg_id in allowed_ids:
			self.client.table("allowed_users").upsert({"tg_id": tg_id, "active": True}).execute()

	def is_allowed(self, tg_id: int) -> bool:
		try:
			res = self.client.table("allowed_users").select("tg_id, active").eq("tg_id", tg_id).maybe_single().execute()
			row = getattr(res, "data", None)
			return bool(row and row.get("active", False))
		except Exception:
			return False

	def get_or_register_employee(self, tg_id: int) -> Optional[Employee]:
		try:
			# Try get existing
			res = self.client.table("employees").select("tg_id, agent_name, active").eq("tg_id", tg_id).maybe_single().execute()
			row = getattr(res, "data", None)
			if row:
				return Employee(tg_id=int(row["tg_id"]), agent_name=row["agent_name"], active=bool(row.get("active", True)))
			# Upsert if missing (idempotent) — execute first, then select separately (avoid chaining .select on upsert)
			self.client.table("employees").upsert({"tg_id": tg_id}, on_conflict="tg_id").execute()
			sel_after_upsert = self.client.table("employees").select("tg_id, agent_name, active").eq("tg_id", tg_id).maybe_single().execute()
			row2 = getattr(sel_after_upsert, "data", None)
			if row2:
				return Employee(tg_id=int(row2["tg_id"]), agent_name=row2["agent_name"], active=bool(row2.get("active", True)))
			# Fallback: explicit insert then select
			try:
				self.client.table("employees").insert({"tg_id": tg_id}).execute()
			except Exception as e_ins:
				try:
					self.log(tg_id, "db_error", {"where": "employees_insert", "error": str(e_ins)})
				except Exception:
					pass
				# continue to select even if insert said conflict
			sel = self.client.table("employees").select("tg_id, agent_name, active").eq("tg_id", tg_id).maybe_single().execute()
			row3 = getattr(sel, "data", None)
			if row3:
				return Employee(tg_id=int(row3["tg_id"]), agent_name=row3["agent_name"], active=bool(row3.get("active", True)))
			# Give up
			self.log(tg_id, "db_error", {"where": "get_or_register_employee", "error": "empty after upsert/insert"})
			return None
		except Exception as e:
			try:
				self.log(tg_id, "db_error", {"where": "get_or_register_employee", "error": str(e)})
			except Exception:
				pass
			return None

	def log(self, tg_id: Optional[int], action: str, payload: Dict[str, Any] | None = None) -> None:
		try:
			self.client.table("logs").insert({
				"tg_id": tg_id,
				"action": action,
				"payload": payload or {},
			}).execute()
		except Exception:
			pass

	# Plans API
	def get_or_create_month_plan(self, tg_id: int, d: date, default_plan: int = 200) -> int:
		year, month = d.year, d.month
		res = self.client.table("sales_plans").select("plan_month").eq("tg_id", tg_id).eq("year", year).eq("month", month).maybe_single().execute()
		row = getattr(res, "data", None)
		if row and "plan_month" in row:
			return int(row["plan_month"])
		self.client.table("sales_plans").upsert({"tg_id": tg_id, "year": year, "month": month, "plan_month": default_plan}, on_conflict="tg_id,year,month").execute()
		return default_plan

	def compute_plan_breakdown(self, tg_id: int, d: date) -> Dict[str, Any]:
		plan_month = self.get_or_create_month_plan(tg_id, d)
		mw = month_workdays(d)
		pd = int(round(plan_month / mw)) if mw > 0 else plan_month
		# week plan = daily plan * number of workdays in this week (Mon-Fri)
		start_week = d - timedelta(days=d.weekday())
		end_week = start_week + timedelta(days=6)
		week_days = count_workdays(start_week, end_week)
		pw = pd * week_days
		# RR = (fact per working days elapsed / elapsed_workdays) * total_workdays_month
		elapsed = month_workdays_elapsed(d)
		today_total, _ = self._sum_attempts_query(tg_id, d.replace(day=1), d)
		rr = int(round((today_total / (elapsed if elapsed > 0 else 1)) * mw)) if mw > 0 else today_total
		return {"plan_month": plan_month, "plan_day": pd, "plan_week": pw, "rr_month": rr, "workdays_month": mw, "workdays_elapsed": elapsed}

	def stats_period(self, tg_id: int, start: date, end: date) -> Dict[str, Any]:
		total, by_product = self._sum_attempts_query(tg_id, start, end)
		return {"total": total, "by_product": by_product}

	def group_ranking_period(self, start: date, end: date) -> List[Dict[str, Any]]:
		res = self.client.table("attempts").select("tg_id, attempt_count").gte("for_date", start.isoformat()).lte("for_date", end.isoformat()).execute()
		sums: Dict[int, int] = {}
		for row in getattr(res, "data", []) or []:
			tg = int(row.get("tg_id"))
			sums[tg] = sums.get(tg, 0) + int(row.get("attempt_count", 0))
		ranking: List[Tuple[int, str, int]] = []
		if sums:
			ids = list(sums.keys())
			emp = self.client.table("employees").select("tg_id, agent_name, active").in_("tg_id", ids).eq("active", True).execute()
			id_to_name = {int(r["tg_id"]): r["agent_name"] for r in (getattr(emp, "data", []) or [])}
			for tg_id, total in sums.items():
				ranking.append((tg_id, id_to_name.get(tg_id, f"agent?{tg_id}"), total))
		ranking.sort(key=lambda x: x[2], reverse=True)
		return [{"tg_id": tg, "agent_name": name, "total": total} for tg, name, total in ranking]

	def create_meet(self, tg_id: int, product_code: str, d: Optional[date] = None) -> Optional[str]:
		"""Create a meeting row and return meet.id (uuid as string)."""
		day = d or date.today()
		try:
			res = self.client.table("meet").insert({
				"tg_id": tg_id,
				"product_code": product_code,
				"for_date": day.isoformat(),
			}).execute()
			data = getattr(res, "data", None) or []
			if data and isinstance(data, list) and data[0].get("id"):
				meet_id = data[0]["id"]
				try:
					self.log(tg_id, "meet_create", {"meet_id": meet_id, "product_code": product_code, "for_date": day.isoformat()})
				except Exception:
					pass
				return str(meet_id)
			# Fallback: select last created
			sel = self.client.table("meet").select("id").eq("tg_id", tg_id).order("created_at", desc=True).limit(1).execute()
			rows = getattr(sel, "data", None) or []
			if rows:
				return str(rows[0]["id"])
			return None
		except Exception as e:
			try:
				self.log(tg_id, "db_error", {"where": "create_meet", "error": str(e)})
			except Exception:
				pass
			return None

	def save_attempts(self, tg_id: int, attempts: Dict[str, int], for_date: date, meet_id: Optional[str] = None) -> None:
		# Ensure employee exists to satisfy FK
		try:
			self.client.table("employees").select("tg_id").eq("tg_id", tg_id).single().execute()
		except Exception:
			self.client.table("employees").insert({"tg_id": tg_id}).execute()
		rows = []
		for product_code, attempt_count in attempts.items():
			if attempt_count <= 0:
				continue
			row: Dict[str, Any] = {
				"tg_id": tg_id,
				"product_code": product_code,
				"attempt_count": attempt_count,
				"for_date": for_date.isoformat(),
			}
			if meet_id:
				row["meet_id"] = meet_id
			rows.append(row)
		if not rows:
			return
		self.client.table("attempts").insert(rows).execute()
		try:
			self.log(tg_id, "save_attempts", {"meet_id": meet_id, "rows": rows})
		except Exception:
			pass

	def _sum_attempts_query(self, tg_id: int, start: date, end: date) -> Tuple[int, Dict[str, int]]:
		res = self.client.table("attempts").select("product_code, attempt_count").eq("tg_id", tg_id).gte("for_date", start.isoformat()).lte("for_date", end.isoformat()).execute()
		total = 0
		by_product: Dict[str, int] = {}
		for row in getattr(res, "data", []) or []:
			c = int(row.get("attempt_count", 0))
			total += c
			pc = row.get("product_code")
			by_product[pc] = by_product.get(pc, 0) + c
		return total, by_product

	def stats_day_week_month(self, tg_id: int, today: date) -> Dict[str, Any]:
		start_week = today - timedelta(days=today.weekday())
		start_month = today.replace(day=1)
		today_total, today_by = self._sum_attempts_query(tg_id, today, today)
		week_total, week_by = self._sum_attempts_query(tg_id, start_week, today)
		month_total, month_by = self._sum_attempts_query(tg_id, start_month, today)
		return {
			"today": {"total": today_total, "by_product": today_by},
			"week": {"total": week_total, "by_product": week_by},
			"month": {"total": month_total, "by_product": month_by},
		}

	def month_ranking(self, month_first_day: date, today: date) -> List[Dict[str, Any]]:
		res = self.client.table("attempts").select("tg_id, attempt_count").gte("for_date", month_first_day.isoformat()).lte("for_date", today.isoformat()).execute()
		sums: Dict[int, int] = {}
		for row in getattr(res, "data", []) or []:
			tg = int(row.get("tg_id"))
			sums[tg] = sums.get(tg, 0) + int(row.get("attempt_count", 0))
		ranking: List[Tuple[int, str, int]] = []
		if sums:
			ids = list(sums.keys())
			emp = self.client.table("employees").select("tg_id, agent_name, active").in_("tg_id", ids).eq("active", True).execute()
			id_to_name = {int(r["tg_id"]): r["agent_name"] for r in (getattr(emp, "data", []) or [])}
			for tg_id, total in sums.items():
				ranking.append((tg_id, id_to_name.get(tg_id, f"agent?{tg_id}"), total))
		ranking.sort(key=lambda x: x[2], reverse=True)
		return [{"tg_id": tg, "agent_name": name, "total": total} for tg, name, total in ranking]

	def day_top_bottom(self, day: date) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
		res = self.client.table("attempts").select("tg_id, attempt_count").eq("for_date", day.isoformat()).execute()
		sums: Dict[int, int] = {}
		for row in getattr(res, "data", []) or []:
			tg = int(row.get("tg_id"))
			sums[tg] = sums.get(tg, 0) + int(row.get("attempt_count", 0))
		if not sums:
			return [], []
		ids = list(sums.keys())
		emp = self.client.table("employees").select("tg_id, agent_name, active").in_("tg_id", ids).eq("active", True).execute()
		id_to_name = {int(r["tg_id"]): r["agent_name"] for r in (getattr(emp, "data", []) or [])}
		pairs = [(tg, id_to_name.get(tg, f"agent?{tg}"), total) for tg, total in sums.items()]
		pairs.sort(key=lambda x: x[2], reverse=True)
		top2 = pairs[:2]
		bottom2 = pairs[-2:] if len(pairs) >= 2 else pairs
		return (
			[{"agent_name": n, "total": t} for _, n, t in top2],
			[{"agent_name": n, "total": t} for _, n, t in bottom2],
		)

	def add_note(self, tg_id: int, content_sanitized: str) -> None:
		# Ensure employee exists to satisfy FK
		try:
			self.client.table("employees").select("tg_id").eq("tg_id", tg_id).single().execute()
		except Exception:
			self.client.table("employees").insert({"tg_id": tg_id}).execute()
		# Force-sanitize before storing
		clean = sanitize_text(content_sanitized)
		self.client.table("notes").insert({"tg_id": tg_id, "content_sanitized": clean}).execute()

	def list_notes(self, tg_id: int, limit: int = 20) -> List[Dict[str, Any]]:
		res = self.client.table("notes").select("created_at, content_sanitized").eq("tg_id", tg_id).order("created_at", desc=True).limit(limit).execute()
		return getattr(res, "data", []) or []

	def list_notes_period(self, tg_id: int, start: date, end: date, limit: int = 50) -> List[Dict[str, Any]]:
		res = self.client.table("notes").select("created_at, content_sanitized").eq("tg_id", tg_id).gte("created_at", datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).isoformat()).lte("created_at", datetime.combine(end, datetime.max.time(), tzinfo=timezone.utc).isoformat()).order("created_at", desc=True).limit(limit).execute()
		return getattr(res, "data", []) or []

	def add_assistant_message(self, tg_id: int, role: str, content_sanitized: str, off_topic: bool = False) -> None:
		# Force-sanitize before storing
		clean = sanitize_text(content_sanitized)
		self.client.table("assistant_messages").insert({"tg_id": tg_id, "role": role, "content_sanitized": clean, "off_topic": off_topic}).execute()

	def get_assistant_messages(self, tg_id: int, limit: int = 20) -> List[Dict[str, Any]]:
		res = self.client.table("assistant_messages").select("role, content_sanitized, off_topic").eq("tg_id", tg_id).order("created_at", desc=True).limit(limit).execute()
		msgs = getattr(res, "data", []) or []
		msgs.reverse()
		return msgs 