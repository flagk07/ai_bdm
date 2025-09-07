from __future__ import annotations

from typing import Any, Dict, List
from datetime import date

from .db import Database
from .pii import sanitize_text_assistant_output


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	"""Neutral assistant: no knowledge/rules. Logs user/assistant messages only."""
	user_clean = sanitize_text_assistant_output(user_message)
	answer = "Ассистент выключен. Опишите задачу — я зафиксирую запрос."
	try:
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", answer, off_topic=False)
	except Exception:
		pass
	return answer 