from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import create_client, Client

from .config import get_settings


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
			if not row:
				# Create if missing (idempotent)
				created = self.client.table("employees").upsert({"tg_id": tg_id}, on_conflict="tg_id").select("tg_id, agent_name, active").maybe_single().execute()
				row = getattr(created, "data", None)
			if not row:
				self.log(tg_id, "db_error", {"where": "get_or_register_employee", "error": "empty data after upsert"})
				return None
			return Employee(tg_id=int(row["tg_id"]), agent_name=row["agent_name"], active=bool(row.get("active", True)))
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

	def save_attempts(self, tg_id: int, attempts: Dict[str, int], for_date: date) -> None:
		# Ensure employee exists to satisfy FK
		try:
			self.client.table("employees").select("tg_id").eq("tg_id", tg_id).single().execute()
		except Exception:
			self.client.table("employees").insert({"tg_id": tg_id}).execute()
		rows = []
		for product_code, attempt_count in attempts.items():
			if attempt_count <= 0:
				continue
			rows.append({
				"tg_id": tg_id,
				"product_code": product_code,
				"attempt_count": attempt_count,
				"for_date": for_date.isoformat(),
			})
		if not rows:
			return
		self.client.table("attempts").insert(rows).execute()

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
		self.client.table("notes").insert({"tg_id": tg_id, "content_sanitized": content_sanitized}).execute()

	def list_notes(self, tg_id: int, limit: int = 20) -> List[Dict[str, Any]]:
		res = self.client.table("notes").select("created_at, content_sanitized").eq("tg_id", tg_id).order("created_at", desc=True).limit(limit).execute()
		return getattr(res, "data", []) or []

	def add_assistant_message(self, tg_id: int, role: str, content_sanitized: str, off_topic: bool = False) -> None:
		self.client.table("assistant_messages").insert({"tg_id": tg_id, "role": role, "content_sanitized": content_sanitized, "off_topic": off_topic}).execute()

	def get_assistant_messages(self, tg_id: int, limit: int = 20) -> List[Dict[str, Any]]:
		res = self.client.table("assistant_messages").select("role, content_sanitized, off_topic").eq("tg_id", tg_id).order("created_at", desc=True).limit(limit).execute()
		msgs = getattr(res, "data", []) or []
		msgs.reverse()
		return msgs 