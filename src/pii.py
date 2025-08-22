import re
from typing import Optional

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
TG_MENTION_RE = re.compile(r"@\w+")

# Numbers and IDs
CARD_RE = re.compile(r"\b\d{13,19}\b")  # bank cards
PASSPORT_RE = re.compile(r"\b\d{4}\s?\d{6}\b")  # RU passport: 4+6 digits
SNILS_RE = re.compile(r"\b\d{3}[\s-]?\d{3}[\s-]?\d{3}[\s-]?\d{2}\b")
INN_RE = re.compile(r"\b\d{10}\b|\b\d{12}\b")
DOB_RE = re.compile(r"\b(0?[1-9]|[12]\d|3[01])[.\-/](0?[1-9]|1[0-2])[.\-/](19|20)\d{2}\b")
# Phone-like: allow digits and mask chars with separators, length >=7
MASKED_DIGITS_RE = re.compile(r"(?:(?:[\dXx*][\s\-().]){6,}[\dXx*])")
PHONE_RE = re.compile(r"(?:\+?\d[\s\-().]?){7,15}")

# Cyrillic names: 2-3 capitalized words
NAME_RE = re.compile(r"\b[А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+){1,2}\b")


def sanitize_text(text: Optional[str]) -> str:
	if not text:
		return ""
	clean = str(text)
	# Emails, mentions
	clean = EMAIL_RE.sub("[email]", clean)
	clean = TG_MENTION_RE.sub("[mention]", clean)
	# Specific identifiers
	clean = DOB_RE.sub("[date]", clean)
	clean = PASSPORT_RE.sub("[passport]", clean)
	clean = SNILS_RE.sub("[snils]", clean)
	clean = INN_RE.sub("[inn]", clean)
	clean = CARD_RE.sub("[number]", clean)
	# Phones and masked digit sequences
	clean = PHONE_RE.sub("[phone]", clean)
	clean = MASKED_DIGITS_RE.sub("[number]", clean)
	# Names (Cyrillic)
	clean = NAME_RE.sub("[name]", clean)
	# Collapse whitespace
	clean = re.sub(r"\s{2,}", " ", clean).strip()
	return clean 