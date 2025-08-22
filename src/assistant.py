from __future__ import annotations

from typing import Any, Dict, List

from openai import OpenAI

from .config import get_settings
from .pii import sanitize_text
from .db import Database


def _build_system_prompt(agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], notes_preview: str) -> str:
	best_today = ", ".join([f"{r['agent_name']}" for r in group_month_ranking[:2]]) if group_month_ranking else "нет данных"
	system = (
		"Ты — AI BDM‑коуч для полевых сотрудников банка. Общайся конкретно и по делу,"
		"держи низкую эмоциональность. Помогай с тактикой кросс‑продаж, ставь достижимые цели,"
		"подсвечивай отставание/перевыполнение плана. Избегай ПДн.\n"
		"Строго ограничься следующими темами: банковские продукты, сценарии кросс‑продаж,"
		"скрипты коммуникации с клиентом, статистика и результаты менеджера, групповая статистика,"
		"постановка целей и рекомендации по улучшению показателей.\n"
		"Если вопрос вне этих тем (история, политика, программирование, общие знания и пр.),"
		"не отвечай по сути — мягко перенаправь разговор в рабочее русло, предложи обсудить"
		"актуальные встречи, продукты или план действий.\n"
		f"Агент: {agent_name}.\n"
		f"Его результаты сегодня: {user_stats.get('today', {}).get('total', 0)} попыток.\n"
		f"Лидеры группы сегодня: {best_today}.\n"
		f"Недавние заметки агента:\n{notes_preview}"
	)
	return system


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	settings = get_settings()
	client = OpenAI(api_key=settings.openai_api_key)

	# Context from DB
	history = db.get_assistant_messages(tg_id, limit=20)
	notes = db.list_notes(tg_id, limit=3)
	notes_preview = "\n".join([f"- {n['content_sanitized']}" for n in notes]) if notes else "—"
	messages: List[Dict[str, str]] = []
	messages.append({"role": "system", "content": _build_system_prompt(agent_name, user_stats, group_month_ranking, notes_preview)})
	for m in history:
		messages.append({"role": m["role"], "content": m["content_sanitized"]})

	user_clean = sanitize_text(user_message)
	messages.append({"role": "user", "content": user_clean})

	resp = client.chat.completions.create(
		model="gpt-4o-mini",
		messages=messages,
		temperature=0.2,
		max_tokens=400,
	)
	answer = resp.choices[0].message.content or ""
	answer_clean = sanitize_text(answer)
	# Persist
	db.add_assistant_message(tg_id, "user", user_clean)
	db.add_assistant_message(tg_id, "assistant", answer_clean)
	return answer_clean 