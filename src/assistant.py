from __future__ import annotations

from typing import Any, Dict, List, Tuple
from datetime import date, datetime, timedelta

from openai import OpenAI

from .config import get_settings
from .pii import sanitize_text
from .db import Database


ALLOWED_TOPICS_HINT = (
	"банковские продукты; кросс‑продажи; скрипты; статистика; цели; план действий"
)


def _build_system_prompt(agent_name: str, stats_line: str, group_line: str, notes_preview: str) -> str:
	system = (
		"Ты — AI BDM‑коуч. Пиши кратко и по делу. Перечисления — нумерованные строки '1. ...'."
		"Без жирного/эмодзи. Темы: продукты банка, кросс‑продажи, скрипты, статистика, цели, план.\n"
		f"Текущие данные: {stats_line}. {group_line}\n"
		f"Заметки сотрудника:\n{notes_preview}"
	)
	return system


def _parse_period(user_text: str, today: date) -> Tuple[date, date, str]:
	low = user_text.lower()
	# Explicit date range dd.mm.yyyy - dd.mm.yyyy
	import re
	m = re.search(r"(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4})\s*[–\-]\s*(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4})", low)
	if m:
		def to_d(s: str) -> date:
			parts = re.split(r"[.\-/]", s)
			day, mon, year = map(int, parts)
			return date(year, mon, day)
		start = to_d(m.group(1))
		end = to_d(m.group(2))
		return start, end, f"период {m.group(1)}–{m.group(2)}"
	if "сегодня" in low:
		return today, today, "сегодня"
	if "вчера" in low:
		y = today - timedelta(days=1)
		return y, y, "вчера"
	if "недел" in low:
		start_week = today - timedelta(days=today.weekday())
		return start_week, today, "текущая неделя"
	if "месяц" in low:
		start_month = today.replace(day=1)
		return start_month, today, "текущий месяц"
	# default: today
	return today, today, "сегодня"


def _is_stats_request(text: str) -> bool:
	low = text.lower()
	keys = ["статист", "итог", "лидер", "рейтинг", "сколько сделал", "по продуктам"]
	return any(k in low for k in keys)


def _is_off_topic(text: str) -> bool:
	low = text.lower().strip()
	if low.isdigit():
		return False
	keywords = [
		"кн", "ксп", "пу", "дк", "ик", "изп", "нс", "вклад", "кн к зп",
		"продаж", "кросс", "скрипт", "возражен", "статист", "план", "цель", "клиент",
	]
	for k in keywords:
		if k in low:
			return False
	off_cues = [
		"погода", "трамп", "президент", "регрессия", "кино", "игра", "анекдот",
		"кто такой", "кто такая", "что такое", "алла", "пугачева", "пугачёва",
	]
	for c in off_cues:
		if c in low:
			return True
	return True


def _format_stats_reply(period_label: str, total: int, by_product: Dict[str, int], leaders: List[Dict[str, Any]]) -> str:
	# Sort products by desc count, show all non-zero; if none, show "нет"
	items = [(p, c) for p, c in by_product.items() if c > 0]
	items.sort(key=lambda x: x[1], reverse=True)
	products_str = ", ".join([f"{p}:{c}" for p, c in items]) if items else "нет"
	leaders_str = ", ".join([f"{r['agent_name']}:{r['total']}" for r in leaders[:3]]) if leaders else "нет"
	return (
		f"1. Период: {period_label} 📅\n"
		f"2. Итого попыток: {total} 🎯\n"
		f"3. По продуктам: {products_str} 📊\n"
		f"4. Лидеры группы: {leaders_str} 🏅"
	)


def _redirect_reply() -> str:
	return (
		"Это вне рабочих тем. Вернёмся к делу: продукты, кросс‑продажи, скрипты, статистика.\n"
		"1. Разбор встречи\n2. Цель на день/неделю\n3. План по продуктам"
	)


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	settings = get_settings()
	client = OpenAI(api_key=settings.openai_api_key)

	user_clean = sanitize_text(user_message)
	today = date.today()
	start, end, period_label = _parse_period(user_clean, today)

	# Early off-topic block
	off_topic = _is_off_topic(user_clean)
	if off_topic:
		redirect = _redirect_reply()
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=True)
		db.add_assistant_message(tg_id, "assistant", sanitize_text(redirect), off_topic=False)
		return redirect

	# Period data
	period_stats = db.stats_period(tg_id, start, end)
	group_rank = db.group_ranking_period(start, end)

	# Direct stats reply with emojis if requested
	if _is_stats_request(user_clean):
		reply = _format_stats_reply(period_label, int(period_stats.get("total", 0)), period_stats.get("by_product", {}), group_rank)
		reply_clean = sanitize_text(reply)
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", reply_clean, off_topic=False)
		return reply_clean

	# Notes only from employee for context
	notes = db.list_notes_period(tg_id, start, end, limit=3)
	notes_preview = "\n".join([f"{i+1}. {n['content_sanitized']}" for i, n in enumerate(notes)]) if notes else "—"

	# Compose messages for model
	stats_line = f"{period_label}: всего {period_stats['total']}; по продуктам {period_stats['by_product']}"
	best = ", ".join([f"{r['agent_name']}:{r['total']}" for r in group_rank[:2]]) if group_rank else "нет данных"
	group_line = f"Лидеры группы за {period_label}: {best}"
	messages: List[Dict[str, str]] = []
	messages.append({"role": "system", "content": _build_system_prompt(agent_name, stats_line, group_line, notes_preview)})
	# Keep last chat history minimal to avoid polluting topic; include last 5
	history = db.get_assistant_messages(tg_id, limit=5)
	for m in history:
		messages.append({"role": m["role"], "content": m["content_sanitized"]})
	messages.append({"role": "user", "content": user_clean})

	resp = client.chat.completions.create(
		model="gpt-4o-mini",
		messages=messages,
		temperature=0.2,
		max_tokens=350,
	)
	answer = resp.choices[0].message.content or ""
	answer_clean = sanitize_text(answer)

	# Store
	db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
	db.add_assistant_message(tg_id, "assistant", answer_clean, off_topic=False)
	return answer_clean 