from __future__ import annotations

from src.db import Database


def main() -> None:
	# New Telegram ID to allow
	tg_id = 1001886119
	db = Database()
	# Grant access
	db.client.table("allowed_users").upsert({"tg_id": tg_id, "active": True}).execute()
	print(f"Added to allowed_users: {tg_id}")


if __name__ == "__main__":
	main() 