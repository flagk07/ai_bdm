from __future__ import annotations

from typing import Any, Dict, List

from openai import OpenAI

from .config import get_settings
from .pii import sanitize_text
from .db import Database


ALLOWED_TOPICS_HINT = (
	"банковские продукты; кросс‑продажи; скрипты; статистика; цели; план действий"
)


def _build_system_prompt(agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], notes_preview: str) -> str:
	best_today = ", ".join([f"{r['agent_name']}" for r in group_month_ranking[:2]]) if group_month_ranking else "нет данных"
	system = (
		"Ты — AI BDM‑коуч. Отвечай кратко. Если перечисление — используй строки, начинающиеся с '- ',"
		"без жирного, эмодзи и лишних символов. Формулировки прямые, без воды.\n"
		"Темы строго: продукты банка, кросс‑продажи, скрипты, статистика, цели, план.\n"
		"Оффтоп перенаправляй к рабочим темам.\n"
		f"Агент: {agent_name}. Сегодня попыток: {user_stats.get('today', {}).get('total', 0)}. Лидеры: {best_today}.\n"
		f"Недавние заметки:\n{notes_preview}"
	)
	return system


def _is_off_topic(text: str) -> bool:
	low = text.lower().strip()
	# Accept pure numeric answer (menu selection like "4" or "10")
	if low.isdigit():
		return False
	keywords = [
		"кн", "ксп", "пу", "дк", "ик", "изп", "нс", "вклад", "кн к зп",
		"продаж", "кросс", "скрипт", "возражен", "статист", "план", "цель", "клиент",
	]
	for k in keywords:
		if k in low:
			return False
	# Common off-topic patterns and cues
	off_cues = [
		"погода", "трамп", "президент", "регрессия", "кино", "игра", "анекдот",
		"кто такой", "кто такая", "что такое", "алла", "пугачева", "пугачёва",
	]
	for c in off_cues:
		if c in low:
			return True
	# Default: off-topic when no allowed keywords
	return True


def _redirect_reply() -> str:
	return (
		"Это вне рабочих тем. Вернёмся к делу: продукты, кросс‑продажи, скрипты, статистика.\n"
		"- Разбор встречи\n- Цель на день/неделю\n- План по продуктам"
	)


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	settings = get_settings()
	client = OpenAI(api_key=settings.openai_api_key)

	# Sanitize early and detect off-topic BEFORE calling the model
	user_clean = sanitize_text(user_message)
	off_topic = _is_off_topic(user_clean)
	if off_topic:
		redirect = _redirect_reply()
		# Persist with off_topic flag, without calling the model
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=True)
		db.add_assistant_message(tg_id, "assistant", sanitize_text(redirect), off_topic=False)
		return redirect

	# Context from DB
	history = db.get_assistant_messages(tg_id, limit=20)
	notes = db.list_notes(tg_id, limit=3)
	notes_preview = "\n".join([f"- {n['content_sanitized']}" for n in notes]) if notes else "—"
	messages: List[Dict[str, str]] = []
	messages.append({"role": "system", "content": _build_system_prompt(agent_name, user_stats, group_month_ranking, notes_preview)})
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

	# Persist
	db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
	db.add_assistant_message(tg_id, "assistant", answer_clean, off_topic=False)
	return answer_clean 