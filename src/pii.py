import re
from typing import Optional

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"(?:\+?\d[\s-]?){7,15}")
CARD_RE = re.compile(r"\b\d{13,19}\b")
TG_MENTION_RE = re.compile(r"@\w+")

# Simple Cyrillic name patterns are hard to detect reliably; we avoid over-removal.


def sanitize_text(text: Optional[str]) -> str:
	if not text:
		return ""
	clean = text
	clean = EMAIL_RE.sub("[email]", clean)
	clean = TG_MENTION_RE.sub("[mention]", clean)
	clean = CARD_RE.sub("[number]", clean)
	# Normalize phone-like sequences
	clean = PHONE_RE.sub("[phone]", clean)
	# Trim excessive whitespace
	clean = re.sub(r"\s{2,}", " ", clean).strip()
	return clean 