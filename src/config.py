import os
from dataclasses import dataclass
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()


def _parse_allowed_ids(raw: Optional[str]) -> List[int]:
	if not raw:
		return []
	ids: List[int] = []
	for part in raw.split(','):
		part = part.strip()
		if not part:
			continue
		try:
			ids.append(int(part))
		except ValueError:
			continue
	return ids


@dataclass
class Settings:
	telegram_bot_token: str
	openai_api_key: str
	supabase_url: str
	supabase_api_key: str
	allowed_tg_ids_bootstrap: List[int]
	timezone: str


def get_settings() -> Settings:
	return Settings(
		telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
		openai_api_key=os.getenv("OPENAI_API_KEY", ""),
		supabase_url=os.getenv("SUPABASE_URL", ""),
		supabase_api_key=os.getenv("SUPABASE_API_KEY", ""),
		allowed_tg_ids_bootstrap=_parse_allowed_ids(os.getenv("ALLOWED_TG_IDS")),
		timezone=os.getenv("APP_TIMEZONE", "Europe/Moscow"),
	) 