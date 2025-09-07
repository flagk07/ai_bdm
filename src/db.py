from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from supabase import create_client, Client

from .config import get_settings
from .pii import sanitize_text

# In-memory fallback for assistant slots (per-process)
_LOCAL_SLOTS: Dict[int, Dict[str, Any]] = {}


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

	def meets_period_count(self, tg_id: int, start: date, end: date) -> int:
		try:
			res = self.client.table("meet").select("id").eq("tg_id", tg_id).gte("for_date", start.isoformat()).lte("for_date", end.isoformat()).execute()
			rows = getattr(res, "data", []) or []
			return len(rows)
		except Exception:
			return 0

	def attempts_linked_period_count(self, tg_id: int, start: date, end: date) -> int:
		try:
			res = self.client.table("attempts").select("id, meet_id, for_date").eq("tg_id", tg_id).gte("for_date", start.isoformat()).lte("for_date", end.isoformat()).execute()
			rows = getattr(res, "data", []) or []
			return sum(1 for r in rows if r.get("meet_id"))
		except Exception:
			return 0

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
			# Ensure employee exists (FK)
			try:
				self.client.table("employees").select("tg_id").eq("tg_id", tg_id).single().execute()
			except Exception:
				self.client.table("employees").upsert({"tg_id": tg_id}, on_conflict="tg_id").execute()
			# Insert first (no select chaining to avoid client limitations)
			self.client.table("meet").insert({
				"tg_id": tg_id,
				"product_code": product_code,
				"for_date": day.isoformat(),
			}).execute()
			# Then select the most recent matching row for this user/day/product
			sel = (
				self.client.table("meet")
				.select("id")
				.eq("tg_id", tg_id)
				.eq("for_date", day.isoformat())
				.eq("product_code", product_code)
				.order("created_at", desc=True)
				.limit(1)
				.execute()
			)
			rows = getattr(sel, "data", None) or []
			if rows:
				meet_id = rows[0]["id"]
				try:
					self.log(tg_id, "meet_create", {"meet_id": meet_id, "product_code": product_code, "for_date": day.isoformat()})
				except Exception:
					pass
				return str(meet_id)
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

	def add_assistant_message(self, tg_id: int, role: str, content_sanitized: str, off_topic: bool = False, auto: bool = False) -> None:
		# Force-sanitize before storing
		clean = sanitize_text(content_sanitized)
		self.client.table("assistant_messages").insert({"tg_id": tg_id, "role": role, "content_sanitized": clean, "off_topic": off_topic, "auto": auto}).execute()

	def get_assistant_messages(self, tg_id: int, limit: int = 20) -> List[Dict[str, Any]]:
		res = self.client.table("assistant_messages").select("role, content_sanitized, off_topic, auto").eq("tg_id", tg_id).order("created_at", desc=True).limit(limit).execute()
		msgs = getattr(res, "data", []) or []
		msgs.reverse()
		return msgs

	# Product rates (FACTS)
	def product_rates_upsert(self, rows: List[Dict[str, Any]]) -> None:
		if not rows:
			return
		self.client.table("depo_rates").insert(rows).execute()

	def product_rates_query(
		self,
		payout_type: Optional[str],
		term_days: Optional[int],
		amount: Optional[float],
		when: Optional[date] = None,
		channel: Optional[str] = None,
		currency: Optional[str] = None,
		source_like: Optional[str] = None,
	) -> List[Dict[str, Any]]:
		q = (
			self.client
			.table("depo_rates")
			.select(
				"id, product_code, plan_name, payout_type, term_days, amount_min, amount_max, amount_inclusive_end, rate_percent, channel, currency, effective_from, effective_to, source_url, source_page, doc_id"
			)
			.eq("product_code", "Вклад")
		)
		if payout_type:
			q = q.eq("payout_type", payout_type)
		if term_days is not None:
			q = q.eq("term_days", term_days)
		if amount is not None:
			q = q.lte("amount_min", amount)
			q = q.or_(f"amount_max.is.null,amount_max.gte.{amount}")
		if channel:
			q = q.eq("channel", channel)
		if currency:
			q = q.eq("currency", currency)
		if source_like:
			q = q.ilike("source_url", source_like)
		# Date filters are optional; if not provided, return latest rows indifferent to date
		if when is not None:
			q = q.lte("effective_from", when.isoformat())
			q = q.or_(f"effective_to.is.null,effective_to.gte.{when.isoformat()}")
		res = q.execute()
		return getattr(res, "data", []) or [] 

	# New: distinct available term_days for product
	def distinct_terms(self, product_code: str) -> List[int]:
		try:
			if product_code == "Вклад":
				res = self.client.table("depo_rates").select("term_days").eq("product_code", "Вклад").order("term_days").execute()
				vals = sorted({int(r.get("term_days", 0)) for r in (getattr(res, "data", []) or []) if r.get("term_days")})
				return vals
			# No generic facts table; return empty for other products
			return []
		except Exception:
			return []

	# New: select_facts for any product (Вклад -> depo_rates; others -> none, use RAG)
	def select_facts(self, product: str, slots: Dict[str, Any]) -> List[Dict[str, Any]]:
		when: Optional[date] = None
		channel = slots.get("channel")
		currency = slots.get("currency")
		term_days = slots.get("term_days")
		amount = slots.get("amount")
		payout_type = slots.get("payout_type")
		try:
			if product == "Вклад":
				return self.product_rates_query(payout_type, term_days, amount, when, channel, currency, None)
			# No generic facts for other products at the moment
			return []
		except Exception:
			return []

	# New: simple RAG docs fetch by product_code (no rag_chunks usage)
	def select_rag_docs_by_product(self, product_code: str, limit: int = 6, sales_stage: Optional[str] = None) -> List[Dict[str, Any]]:
		try:
			q = (
				self.client
				.table("rag_docs")
				.select("id, url, title, content, product_code, sales_stage")
				.eq("product_code", product_code)
			)
			if sales_stage:
				try:
					q = q.eq("sales_stage", sales_stage)
				except Exception:
					pass
			res = q.order("fetched_at", desc=True).limit(limit).execute()
			return getattr(res, "data", []) or []
		except Exception:
			return []

	# New: search playbook passages via RPC (docs/doc_passages)
	def search_playbook(self, query: str, product: str = "Плейбук", limit: int = 8) -> List[Dict[str, Any]]:
		"""Call search_passages RPC. Returns rows with keys: passage_id, ord, section, anchor, snippet, rank."""
		try:
			res = self.client.rpc("search_passages", {"p_product": product, "p_query": query, "p_limit": limit}).execute()
			return getattr(res, "data", []) or []
		except Exception:
			return []

	# New: select RAG rules by doc_ids (deprecated: rag_chunks removed) — keep for backward compatibility to return empty
	def select_rag_rules(self, doc_ids: set[str], limit: int = 6, no_numbers: bool = True) -> List[Dict[str, Any]]:
		# rag_chunks removed; return empty to force assistant to use select_rag_docs_by_product
		return []

	# Assistant slots
	def get_slots(self, tg_id: int) -> Dict[str, Any]:
		try:
			res = self.client.table("assistant_slots").select("product_code,payout_type,currency,term_days,amount,channel").eq("tg_id", tg_id).maybe_single().execute()
			row = getattr(res, "data", {}) or {}
			if row:
				return row
		except Exception:
			pass
		# fallback
		return _LOCAL_SLOTS.get(int(tg_id), {})

	def set_slots(self, tg_id: int, **kwargs: Any) -> None:
		data = {k: v for k, v in kwargs.items() if v is not None}
		if not data:
			return
		data["tg_id"] = int(tg_id)
		# try DB first
		try:
			self.client.table("assistant_slots").upsert(data, on_conflict="tg_id").execute()
		except Exception:
			pass
		# always update local fallback
		cur = _LOCAL_SLOTS.get(int(tg_id), {})
		cur.update({k: v for k, v in data.items() if k != "tg_id"})
		_LOCAL_SLOTS[int(tg_id)] = cur

	def clear_slots(self, tg_id: int) -> None:
		try:
			self.client.table("assistant_slots").delete().eq("tg_id", tg_id).execute()
		except Exception:
			pass
		_LOCAL_SLOTS.pop(int(tg_id), None) 