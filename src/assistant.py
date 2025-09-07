from __future__ import annotations

from typing import Any, Dict, List

from openai import OpenAI
import re

from .db import Database
from .pii import sanitize_text_assistant_output
from .config import get_settings


def _normalize_output(text: str) -> str:
	"""Remove markdown '**' and ensure bullets/numbered items start on new lines."""
	if not text:
		return ""
	# drop bold markers
	clean = text.replace("**", "")
	# normalize newlines
	clean = clean.replace("\r\n", "\n").replace("\r", "\n")
	# insert newline before N) bullets if not at start of line
	clean = re.sub(r"(?<!^)\s+(?=\d{1,2}\)\s)", "\n", clean)
	# insert newline before hyphen bullets if not at start of line
	clean = re.sub(r"(?<!^)\s+(?=-\s)", "\n", clean)
	# collapse excessive blank lines
	clean = re.sub(r"\n{3,}", "\n\n", clean)
	return clean.strip()


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	"""Normal dialog: route user message to OpenAI with a concise RU system prompt.
	Adds the last 10 messages of the dialog to preserve context.
	"""
	settings = get_settings()
	user_clean = sanitize_text_assistant_output(user_message)
	# Build messages with short system prompt
	system_prompt = (
		"Ты — полезный ассистент. Отвечай кратко и по делу на русском, без шаблонных фраз и дисклеймеров."
	)
	messages: List[Dict[str, str]] = [{"role": "system", "content": system_prompt}]
	# Pull last 10 messages from history and add to context (role, content)
	try:
		history = db.get_assistant_messages(tg_id, limit=10)
		for m in history:
			role = m.get("role") or "user"
			content = m.get("content_sanitized") or ""
			if role not in ("user", "assistant", "system"):
				role = "user"
			messages.append({"role": role, "content": content})
	except Exception:
		pass
	# Append current user message
	messages.append({"role": "user", "content": user_clean})
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
	answer_clean = _normalize_output(answer_clean)
	try:
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", answer_clean, off_topic=False)
	except Exception:
		pass
	return answer_clean 