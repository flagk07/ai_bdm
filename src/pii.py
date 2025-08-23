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

# Cyrillic FIO patterns (strict)
FIO_STRICT_RE = re.compile(
	r"\b[А-ЯЁ][а-яё]+(?:ов|ев|ёв|ин|ын|ский|цкий|ко|ук|юк|ыч|ая|ий|ый|ская)\s+"
	r"[А-ЯЁ][а-яё]+\s+"
	r"[А-ЯЁ][а-яё]+(?:ович|евич|ич|овна|евна|ична|инична)\b"
)

# Russian number words (tokens) for masked numbers/phones
RUS_NUM_TOKEN = r"(?:ноль|нуль|один|одна|одно|два|две|три|четыре|пять|шесть|семь|восемь|девять|десять|одиннадцать|двенадцать|тринадцать|четырнадцать|пятнадцать|шестнадцать|семнадцать|восемнадцать|девятнадцать|двадцать|тридцать|сорок|пятьдесят|шестьдесят|семьдесят|восемьдесят|девяносто|сто|двести|триста|четыреста|пятьсот|шестьсот|семьсот|восемьсот|девятьсот|тысяч[аеиоуы]*|миллион[аов]*|миллиард[аов]*)"
NUM_TOKEN_RE = re.compile(rf"(?:\d+|[xX*]+|{RUS_NUM_TOKEN})", re.IGNORECASE)
# Sequence: 7+ tokens separated by spaces/punct or 'и'
NUM_SEQ_RE = re.compile(rf"(?:{NUM_TOKEN_RE.pattern})(?:[\s\-–—/().,]*?(?:и\s+)??(?:{NUM_TOKEN_RE.pattern})){6,}", re.IGNORECASE)

# Mixed digits + Russian letters sequence (e.g., "36семь96"), with total digits >=5 inside the run
MIXED_PHONE_LIKE_RE = re.compile(r"(?=(?:.*\d){5,})(?=(?:.*[А-Яа-яЁё]){1,})(?:[\dXx*А-Яа-яЁё]+[\s\-–—/().]?){3,}[\dXx*А-Яа-яЁё]+", re.IGNORECASE)


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
	# Phones and masked digit sequences (digits/X/*)
	clean = PHONE_RE.sub("[phone]", clean)
	clean = MASKED_DIGITS_RE.sub("[phone]", clean)
	# Masked long number sequences using Russian number words and digits
	clean = NUM_SEQ_RE.sub("[phone]", clean)
	# Mixed digits+letters runs resembling phones
	clean = MIXED_PHONE_LIKE_RE.sub("[phone]", clean)
	# Names (strict FIO only)
	clean = FIO_STRICT_RE.sub("[name]", clean)
	# Collapse whitespace
	clean = re.sub(r"\s{2,}", " ", clean).strip()
	return clean 