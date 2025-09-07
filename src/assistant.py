from __future__ import annotations

from typing import Any, Dict, List

from .db import Database
from .pii import sanitize_text_assistant_output


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	"""Minimal dialog: echo sanitized user text back. No rules/knowledge."""
	user_clean = sanitize_text_assistant_output(user_message)
	answer = f"Вы спросили: {user_clean}"
	try:
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", answer, off_topic=False)
	except Exception:
		pass
	return answer 