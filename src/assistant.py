from __future__ import annotations

from typing import Any, Dict, List

from openai import OpenAI

from .db import Database
from .pii import sanitize_text_assistant_output
from .config import get_settings


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	"""Normal dialog: route user message to OpenAI with a concise RU system prompt."""
	settings = get_settings()
	user_clean = sanitize_text_assistant_output(user_message)
	# Build messages
	system_prompt = (
		"Ты — полезный ассистент. Отвечай кратко и по делу на русском, без шаблонных фраз и дисклеймеров."
	)
	messages = [
		{"role": "system", "content": system_prompt},
		{"role": "user", "content": user_clean},
	]
	answer = ""
	try:
		client = OpenAI(api_key=settings.openai_api_key)
		resp = client.chat.completions.create(
			model=settings.assistant_model,
			messages=messages,
			temperature=0.5,
			max_tokens=400,
		)
		answer = resp.choices[0].message.content or ""
	except Exception:
		answer = user_clean or "Ок"
	answer_clean = sanitize_text_assistant_output(answer)
	try:
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", answer_clean, off_topic=False)
	except Exception:
		pass
	return answer_clean 