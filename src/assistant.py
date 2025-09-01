from __future__ import annotations

from typing import Any, Dict, List, Tuple
from datetime import date, datetime, timedelta

from openai import OpenAI

from .config import get_settings
from .pii import sanitize_text, sanitize_text_assistant_output
from .db import Database
import re
import os
from typing import Optional


ALLOWED_TOPICS_HINT = (
	"банковские продукты; кросс‑продажи; скрипты; статистика; цели; план действий"
)


# ------------------------ Deposit rates helpers ------------------------

def _parse_amount_rub(text: str) -> Optional[float]:
	low = text.lower().replace("\u00a0", " ").replace("\u202f", " ")
	# 1) Explicit currency forms
	m = re.search(r"(\d[\d\s]{2,}(?:[.,]\d{1,2})?)\s*(?:руб|₽|rub)", low)
	if m:
		num = m.group(1).replace(" ", "").replace(",", ".")
		try:
			return float(num)
		except Exception:
			return None
	# 2) Word-based multipliers (млн/тыс) without currency
	m2 = re.search(r"(\d+(?:[.,]\d+)?)\s*(млн|миллион|million|m|тыс|тысяч|k)\b", low)
	if m2:
		val = float(m2.group(1).replace(",", "."))
		unit = m2.group(2)
		mult = 1.0
		if unit.startswith("мл") or unit.startswith("mil") or unit == "m":
			mult = 1_000_000.0
		elif unit.startswith("тыс") or unit.startswith("k"):
			mult = 1_000.0
		return val * mult
	# 3) Bare number likely representing RUB (>=5 digits)
	m3 = re.search(r"\b(\d[\d\s]{4,})\b", low)
	if m3:
		try:
			return float(m3.group(1).replace(" ", ""))
		except Exception:
			return None
	return None


def _parse_payout_type(text: str) -> Optional[str]:
	low = text.lower()
	if "ежемесяч" in low or "каждый месяц" in low:
		return "monthly"
	if "в конце" in low or "по окончании" in low or "капитализац" in low:
		return "end"
	return None


def _parse_term_days(text: str) -> Optional[int]:
	low = text.lower()
	# months mapping
	mon_map = {1:31,2:61,3:91,4:122,6:181,9:274,12:367,18:550,24:730,36:1100}
	m_mon = re.search(r"(\d+)\s*(?:мес|месяц|месяца|месяцев)\b", low)
	if m_mon:
		mon = int(m_mon.group(1))
		return mon_map.get(mon, mon * 30)
	m_day = re.search(r"(\d+)\s*(?:дн|дней|day|days)\b", low)
	if m_day:
		return int(m_day.group(1))
	# plain number that looks like days
	m_num = re.search(r"\b(\d{2,4})\b", low)
	if m_num:
		val = int(m_num.group(1))
		if 10 <= val <= 2000:
			return val
	return None


def _try_reply_deposit_rates(db: Database, tg_id: int, user_clean: str, today: date) -> Optional[str]:
	lowq = user_clean.lower()
	# Broaden trigger: treat as deposit rates query if deposit or payout phrasing is present
	if not any(k in lowq for k in ["вклад", "депозит", "ставк", "ежемесяч", "в конце", "капитализац"]):
		return None
	amt = _parse_amount_rub(user_clean)
	pt = _parse_payout_type(user_clean)
	term = _parse_term_days(user_clean)
	# If neither provided, ask a single clarification
	if amt is None and pt is None and term is None:
		return (
			"Уточните, пожалуйста: выплата процентов 1) ежемесячно или 2) в конце срока, сумма (например, 300 000 ₽), и срок (например, 181 дней)."
		)
	# Query rates
	when = None  # do not filter by dates to allow rows with NULL effective_from/to
	# Channel filter for «Мой Дом»
	channel = None
	if "мой дом" in lowq or "интернет-банк" in lowq or "интернет банк" in lowq:
		channel = "Интернет-Банк"
	# Detect currency from query (₽/$/€/¥), else no filter
	curr = _detect_currency(user_clean)
	rows = db.product_rates_query(pt, term, amt, when, channel=channel, currency=curr, source_like=None)
	if not rows:
		# Fallback loosen filters stepwise
		if term is not None:
			rows = db.product_rates_query(pt, None, amt, when, channel=channel, currency=curr, source_like=None)
		if not rows:
			rows = db.product_rates_query(pt, None, amt, None)
	if not rows:
		return "Нет данных о ставках по вкладам для указанных параметров, проверьте первоисточник."
	# If result set is big and user didn't ask to 'show all', ask for clarifications to avoid overly long answer
	if "показать все" not in lowq:
		too_many = len(rows) > 30
		missing_keys = []
		if curr is None:
			missing_keys.append("валюта (RUB/USD/EUR/CNY)")
		if pt is None:
			missing_keys.append("выплата процентов (ежемесячно/в конце)")
		if amt is None:
			missing_keys.append("ориентировочная сумма (например, 1 000 000 ₽)")
		if term is None:
			missing_keys.append("срок (например, 181 дней)")
		# If many rows or missing key filters — ask 1 clarifying message
		if too_many or missing_keys:
			# Build compact hints from data
			terms = sorted({int(r.get("term_days", 0)) for r in rows if r.get("term_days")})
			plans = sorted({(r.get("plan_name") or "").strip() for r in rows if (r.get("plan_name") or "").strip()})
			curropts = sorted({(r.get("currency") or "").strip() for r in rows if (r.get("currency") or "").strip()})
			term_hint = ("; сроки: " + ", ".join(map(str, terms[:10])) + (" …" if len(terms) > 10 else "")) if terms else ""
			plan_hint = ("; тарифы: " + ", ".join(plans[:5]) + (" …" if len(plans) > 5 else "")) if plans else ""
			cur_hint = ("; валюты: " + ", ".join(curropts)) if curropts else ""
			need = "; ".join(missing_keys) if missing_keys else "уточните срок (например, 181 дней)"
			return (
				"Чтобы дать корректный и не слишком длинный ответ, уточните: " + need + ".\n"
				f"Можно ответить одной строкой: ‘ежемесячно, 1 000 000 ₽, 181 дней, RUB’.\nПодсказки{term_hint}{plan_hint}{cur_hint}.\n"
				"Напишите ‘показать все’, если нужен полный список (может быть длинно)."
			)
	# Group by payout_type -> term_days -> amount bucket
	def _fmt_amount(val: Optional[float], curr: Optional[str]) -> str:
		if val is None:
			return ""
		try:
			num = f"{float(val):,.0f}".replace(",", " ")
		except Exception:
			num = str(val)
		if (curr or "").upper() == "RUB":
			return f"{num} ₽"
		return f"{num} {curr or ''}".strip()
	def _bucket(r: Dict[str, Any]) -> str:
		amin = float(r.get("amount_min") or 0)
		amax = r.get("amount_max")
		curr = (r.get("currency") or "").upper() or None
		if amax is None:
			return f"от {_fmt_amount(amin, curr)}"
		return f"{_fmt_amount(amin, curr)}–{_fmt_amount(float(amax), curr)}"
	# Build concise header
	header_parts: List[str] = ["Подбор вкладов"]
	if term is not None:
		header_parts.append(f"на срок {term} дней")
	if pt is not None:
		header_parts.append("с ежемесячной выплатой процентов" if pt == "monthly" else "с выплатой в конце срока")
	if amt is not None:
		header_parts.append(f"на сумму {_fmt_amount(amt, curr)}")
	header = " ".join(header_parts) + ":"
	# Sort by term, then rate desc, then amount_min
	r_sorted = sorted(rows, key=lambda r: (int(r.get("term_days", 0)), -(_rate_pct_of(r)), float(r.get("amount_min") or 0)))
	lines = [header]
	count = 0
	for r in r_sorted:
		term_r = int(r.get("term_days", 0))
		if term is not None and term_r != term:
			continue
		plan = (r.get("plan_name") or "").strip()
		if not plan:
			continue
		ref_src = (r.get("source_url") or "").strip()
		if ref_src and ref_src not in sources:
			sources[ref_src] = len(sources) + 1
			si = sources[ref_src]
		else:
			si = sources.get(ref_src, 1) if ref_src else 1
		lines.append(f"- {plan}: {_rate_pct_of(r):.1f}%" + (f" [S{si}]" if ref_src else ""))
		count += 1
		if count >= MAX_OUTPUT_LINES:
			break
	# Recommend top tariffs separately (numbered)
	top = sorted([r for r in r_sorted if (term is None or int(r.get("term_days", 0)) == term)], key=lambda r: _rate_pct_of(r), reverse=True)[:2]
	reco = ""
	if top:
		reco_lines = []
		for i, t in enumerate(top, start=1):
			pname = (t.get("plan_name") or "").strip()
			reco_lines.append(f"{i}) {pname}: {_rate_pct_of(t):.1f}%")
		reco = "\nРекомендуемое (по ставке):\n" + "\n".join(reco_lines)
	# Actions single line
	actions = "\nДействия сотрудника: выберите наиболее подходящий тариф из списка и помогите открыть вклад клиенту"
	# Only SOURCES block
	src_lines = [f"S{idx}: {url}" for url, idx in sources.items()]
	sources_block = ("\n\nSOURCES:\n" + "\n".join(src_lines)) if src_lines else ""
	return "\n".join(lines) + ("\n" + reco if reco else "") + actions + sources_block


# ------------------------ System prompt builder ------------------------

def _build_system_prompt(agent_name: str, stats_line: str, group_line: str, notes_preview: str) -> str:
	system = (
		# Роль и миссия
		"Ты — AI BDM (Business Development Manager) для выездных сотрудников банка. "
		"Помогаешь только по рабочим вопросам: продукты банка, кросс‑продажи, результаты, цели, план действий, наставничество, повышение качества консультаций и продуктивности. "
		"Сотрудник доставки работает по уже подтверждённым заявкам и делает кросс‑продажи на встрече; он НЕ привлекает клиентов и НЕ управляет командой.\n"
		# Жёсткие рамки (scope)
		"Строго держись рамок. Разрешено: краткие свойства/выгоды/позиционирование продуктов, скрипты и возражения; "
		"результаты сотрудника и команды (попытки), выполнение планов, рейтинг; постановка SMART‑целей, планы, чек‑листы, контрольные точки; "
		"коучинг (конкретные рекомендации, разбор кейсов, тайм‑менеджмент, фокус); улучшение качества продаж (структура встречи, выявление потребностей, презентация выгоды, следующее действие).\n"
		"Запрещено: любые темы вне работы; юридические/налоговые консультации без базы; запрашивать/обрабатывать ПДн клиентов; "
		"придумывать точные тарифы/ставки/требования без предоставленной справки. "
		"Нельзя предлагать привлечение новых клиентов/маркетинг, обучение команды, или управленческие меры — это вне контроля сотрудника доставки. Если данных о продукте не хватает — задай 1 короткое уточнение.\n"
		# Данные из бота
		f"Контекст: {stats_line}. {group_line}\n"
		f"Заметки сотрудника:\n{notes_preview}\n"
		# Язык и стиль
		"Стиль: по делу, деловой и доброжелательный, без воды. Короткие абзацы и нумерованные пункты 1., 2., 3. "
		"Без жирного и эмодзи. Не используй ПДн и не запрашивай их. Если данных не хватает — спроси не больше 1 уточнения.\n"
		"Ты — мастер продаж: владеешь техниками SPIN, потребностями/выгодами, работой с возражениями, апселлом/кросс‑селлом. "
		"В ответах давай короткие, прикладные советы по продажам (формулировки, следующий шаг, фиксация договорённостей) применительно к контексту клиента.\n"
		# Формат
		"Формат ответа по умолчанию (если не просили иначе):\n"
		"1) Сводка (1–2 строки) — что видно и куда двигать.\n"
		"2) Диагностика (2–4 пункта) — что тормозит/что хорошо (по продуктам/этапам).\n"
		"3) Рекомендации (3–6 пунктов) — конкретные шаги/формулировки/фокус‑план.\n"
		"4) План (день/неделя) — SMART‑цели по попыткам/продуктам.\n"
		"5) Контроль — какие метрики посмотреть до следующего контакта.\n"
		# Правила качества и верификации
		"Никаких домыслов о тарифах/условиях — говори обобщённо или проси справку. "
		"Если в системном контексте (RAG) присутствуют точные цифры (ставки, суммы, сроки) — используй их дословно и укажи диапазон/условия так, как в справке. "
		"Не указывай числовые ставки/суммы/сроки, если их нет в RAG‑блоке (извлечённых строк) — вместо этого задай 1 уточнение (валюта/тариф/канал). "
		"Пиши строго продукт-специфично: упоминай продукт(ы) из перечня [КН, КСП, ПУ, ДК, ИК, ИЗП, НС, Вклад, КН к ЗП]; если продукт не указан, уточни. "
		"Не делай общих выводов вида ‘скрипт неэффективен’ — укажи конкретный этап и формулировку, которую улучшить. "
		"Привязывай советы к метрикам (attempts, план/факт, RR) и к заметкам сотрудника. Учитывай предыдущую переписку и ранее выданные рекомендации при формулировке новых.\n"
		# Приоритет источников и цитирование
		"Порядок данных: СНАЧАЛА FACTS (таблица product_rates — ставки/сроки/суммы/валюта), затем RAG (текстовые правила/исключения). "
		"Если спрашивают финансовые условия (ставки, сроки, суммы, тариф) — сначала найди ответ в FACTS. RAG добавляй только для правил (пополнение/частичное снятие/лимиты). "
		"Если в FACTS нет подходящих строк — ответ ищи в RAG; если и там нет — напиши: ‘нет данных, проверьте первоисточник’. "
		"Цитирование: любая цифра должна сопровождаться [F#] (FACTS) и/или [S#] (SOURCES). Валюту указывай рядом со ставкой. При любой неоднозначности задай 1 уточнение.\n"
	)
	return system



def _parse_period(user_text: str, today: date) -> Tuple[date, date, str]:
	low = user_text.lower()
	# Explicit date range dd.mm.yyyy - dd.mm.yyyy
	import re
	m = re.search(r"(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4})\s*[–\-]\s*(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{4})", low)
	if m:
		def to_d(s: str) -> date:
			parts = re.split(r"[.\-/]", s)
			day, mon, year = map(int, parts)
			return date(year, mon, day)
		start = to_d(m.group(1))
		end = to_d(m.group(2))
		return start, end, f"период {m.group(1)}–{m.group(2)}"
	if "сегодня" in low:
		return today, today, "сегодня"
	if "вчера" in low:
		y = today - timedelta(days=1)
		return y, y, "вчера"
	if "недел" in low:
		start_week = today - timedelta(days=today.weekday())
		return start_week, today, "текущая неделя"
	if "месяц" in low:
		start_month = today.replace(day=1)
		return start_month, today, "текущий месяц"
	# default: today
	return today, today, "сегодня"



def _is_stats_request(text: str) -> bool:
	low = text.lower()
	# Ignore internal auto-summary prompts
	if "[auto_summary]" in low:
		return False
	keys = ["статист", "итог", "лидер", "рейтинг", "сколько сделал", "по продуктам"]
	return any(k in low for k in keys)



def _is_off_topic(text: str) -> bool:
	low = text.lower().strip()
	# Numeric menu answer is allowed
	if low.isdigit():
		return False
	# Explicit off-topic cues → True
	off_cues = [
		"погода", "трамп", "президент", "регрессия", "кино", "игра", "анекдот",
		"кто такой", "кто такая", "что такое", "алла", "пугачева", "пугачёва",
	]
	for c in off_cues:
		if c in low:
			return True
	# Default: treat as on-topic
	return False



def _format_stats_reply(period_label: str, total: int, by_product: Dict[str, int], leaders: List[Dict[str, Any]]) -> str:
	# Sort products by desc count, show all non-zero; if none, show "нет"
	items = [(p, c) for p, c in by_product.items() if c > 0]
	items.sort(key=lambda x: x[1], reverse=True)
	products_str = ", ".join([f"{p}:{c}" for p, c in items]) if items else "нет"
	leaders_str = ", ".join([f"{r['agent_name']}:{r['total']}]" for r in leaders[:3]]) if leaders else "нет"
	return (
		f"1. Период: {period_label} 📅\n"
		f"2. Итого попыток: {total} 🎯\n"
		f"3. По продуктам: {products_str} 📊\n"
		f"4. Лидеры группы: {leaders_str} 🏅"
	)



def _redirect_reply() -> str:
	return (
		"Это вне рабочих тем. Вернёмся к делу: продукты, кросс‑продажи, скрипты, статистика.\n"
		"1. Разбор встречи\n2. Цель на день/неделю\n3. План по продуктам"
	)



def _normalize_bullets(text: str) -> str:
	"""Ensure that numbered bullets '1)', '2)' (and legacy '1.') start on new lines.
	- Inserts a newline before any occurrence of '<digits>) ' or '<digits>. ' not already at line start.
	- Ensures '-' sub-bullets start on a new line.
	- If a numbered item has exactly one immediate sub-bullet, inline it on the same line without '-'.
	- Collapses extra spaces around newlines.
	"""
	if not text:
		return ""
	# Normalize newlines first
	normalized = text.replace("\r\n", "\n").replace("\r", "\n")
	# Insert newline before N) or N. where N=1..99 if not already at start of line
	normalized = re.sub(r"(?<!^)\s+(?=\d{1,2}\)\s)", "\n", normalized)
	normalized = re.sub(r"(?<!^)\s+(?=\d{1,2}\.\s)", "\n", normalized)
	# Insert newline before hyphen bullets "- " when not at line start
	normalized = re.sub(r"(?<!^)\s+(?=-\s)", "\n", normalized)
	# Trim trailing spaces per line
	lines = [ln.strip() for ln in normalized.split("\n") if ln.strip()]
	# Inline single sub-bullet into its parent numbered item
	result: List[str] = []
	i = 0
	while i < len(lines):
		line = lines[i]
		m_num = re.match(r"^(\d{1,2}\))\s+(.*)", line)
		if not m_num:
			# also support legacy '1.' pattern
			m_num = re.match(r"^(\d{1,2})\.\s+(.*)", line)
			if m_num:
				# convert to 1) style
				num = m_num.group(1) + ")"
				main = m_num.group(2)
				# check next line for single sub-bullet
				if i + 1 < len(lines):
					m_sub = re.match(r"^-\s+(.*)", lines[i + 1])
					# ensure only one sub-bullet (next next starts new numbered or end)
					is_single = False
					if m_sub:
						if (i + 2 >= len(lines)) or re.match(r"^(\d{1,2})[)\.]\s+", lines[i + 2]):
							is_single = True
					if m_sub and is_single:
						result.append(f"{num} {main} {m_sub.group(1)}")
						i += 2
						continue
					else:
						result.append(f"{num} {main}")
						i += 1
						continue
			else:
				# not a numbered line, keep as-is
				result.append(line)
				i += 1
				continue
		# Here we have 1) pattern already
		num = m_num.group(1)
		main = m_num.group(2)
		if i + 1 < len(lines):
			m_sub2 = re.match(r"^-\s+(.*)", lines[i + 1])
			is_single2 = False
			if m_sub2:
				if (i + 2 >= len(lines)) or re.match(r"^(\d{1,2})[)\.]\s+", lines[i + 2]):
					is_single2 = True
			if m_sub2 and is_single2:
				result.append(f"{num} {main} {m_sub2.group(1)}")
				i += 2
				continue
		# default: just append numbered line
		result.append(f"{num} {main}")
		i += 1
	# Join back
	return "\n".join(result).strip()



def _strip_md_emphasis(text: str) -> str:
	"""Remove markdown emphasis like **bold** or *italic* without touching bullets."""
	if not text:
		return ""
	import re as _re
	# **bold** -> bold
	text = _re.sub(r"\*\*(.*?)\*\*", r"\1", text)
	# *italic* -> italic (avoid converting list markers)
	text = _re.sub(r"(?<!^)\*(?!\s)([^*]+?)\*(?!\S)", r"\1", text, flags=_re.MULTILINE)
	# Remove stray double-asterisks
	return text.replace("**", "")


CURRENCY_HINTS = {
	"RUB": ["руб", "₽", "rub", "в руб", "руб."],
	"USD": ["usd", "$", "доллар"],
	"EUR": ["eur", "€", "евро"],
	"CNY": ["cny", "¥", "юан", "юани"],
}


def _detect_currency(query: str) -> Optional[str]:
	low = query.lower().replace('\u00a0',' ').replace(' ', '')
	for code, keys in CURRENCY_HINTS.items():
		for k in keys:
			kk = k.replace(' ', '')
			if kk in low:
				return code
	return None


def _vector_top_chunks(db: Database, product: Optional[str], currency: Optional[str], query: str, k: int = 5) -> list[Dict[str, Any]]:
	"""Vector search via RPC if embeddings are present. Returns rows with content/currency/product_code/distance."""
	try:
		# simple embedding using same model as ingestion
		client = OpenAI(api_key=get_settings().openai_api_key)
		e = client.embeddings.create(model="text-embedding-3-small", input=query)
		emb = e.data[0].embedding
		res = db.client.rpc(
			"match_rag_chunks",
			{"product": product, "currency_in": currency, "query_embedding": emb, "match_count": k},
		).execute()
		rows = getattr(res, "data", []) or []
		return rows
	except Exception:
		return []


def _rag_snippets(db: Database, product_hint: Optional[str], limit: int = 5) -> List[Dict[str, str]]:
	"""Fetch top RAG snippets from rag_docs by product_code or keyword in title/content.
	Simple heuristic until pgvector is added: filter by product_code, else keyword in content.
	"""
	try:
		if product_hint:
			res = db.client.table("rag_docs").select("id,url,title,content,product_code").ilike("product_code", product_hint).order("fetched_at", desc=True).limit(limit).execute()
			rows = getattr(res, "data", []) or []
			if rows:
				return [{"url": r.get("url",""), "title": r.get("title",""), "content": r.get("content",""), "product_code": r.get("product_code",""), "id": r.get("id") } for r in rows]
		# fallback: recent docs
		res2 = db.client.table("rag_docs").select("id,url,title,content,product_code").order("fetched_at", desc=True).limit(limit).execute()
		rows2 = getattr(res2, "data", []) or []
		return [{"url": r.get("url",""), "title": r.get("title",""), "content": r.get("content",""), "product_code": r.get("product_code",""), "id": r.get("id") } for r in rows2]
	except Exception:
		return []



def _extract_rate_lines(text: str) -> list[str]:
	"""Extract lines with percent patterns to guide the model toward concrete rates."""
	lines = [l.strip() for l in text.split('\n') if l.strip()]
	res: list[str] = []
	import re as _re
	for l in lines:
		if _re.search(r"\d{1,2}(?:[.,]\d)?\s*%", l):
			res.append(l)
	return res[:6]



def _rag_top_chunks(db: Database, product_hint: Optional[str], query: str, limit_docs: int = 3, limit_chunks: int = 5) -> Tuple[List[str], Dict[str, Any]]:
	"""Pick top chunks for product with currency awareness.
	Returns (texts, meta) where meta contains currencies set and extracted rate lines and sources list.
	Order: vector search (product/currency filters) → keyword fallback → doc content fallback.
	"""
	currency = _detect_currency(query)
	# 1) vector search with strict product filter (no cross-product fallback)
	vec_rows = _vector_top_chunks(db, product_hint, currency, query, k=limit_chunks)
	if vec_rows:
		texts = [r.get("content", "") for r in vec_rows if r.get("content")]
		currs = {r.get("currency") for r in vec_rows if r.get("currency")}
		rate_lines: list[str] = []
		for t in texts:
			for rl in _extract_rate_lines(t):
				rate_lines.append(rl)
		meta = {"currencies": list({c for c in currs if c}), "rates": rate_lines[:10], "via": "vector", "sources": []}
		return texts, meta
	# keywords from query: words >=3 chars
	words = [w for w in re.findall(r"[А-Яа-яA-Za-z0-9%]+", query.lower()) if len(w) >= 3]
	ids: List[str] = []
	base_docs: List[Dict[str, Any]] = []
	try:
		base = _rag_snippets(db, product_hint, limit=limit_docs)
		base_docs = base
		ids = [r.get("id") for r in base if r.get("id")]
	except Exception:
		ids = []
		base_docs = []
	chunks: List[Dict[str, str]] = []
	if ids:
		try:
			res = db.client.table("rag_chunks").select("content, chunk_index, product_code, doc_id, currency").in_("doc_id", ids).limit(200).execute()
			rows = getattr(res, "data", []) or []
			for r in rows:
				chunks.append({"content": r.get("content",""), "chunk_index": int(r.get("chunk_index", 0)), "currency": r.get("currency")})
		except Exception:
			chunks = []
	if not chunks:
		# fallback: first 1200 of docs
		docs = _rag_snippets(db, product_hint, limit=limit_docs)
		texts = [d.get("content","")[:1200] for d in docs if d.get("content")] [:limit_chunks]
		meta = {"currencies": [], "rates": [], "via": "docs", "sources": [{"title": d.get("title",""), "url": d.get("url",""), "id": d.get("id")} for d in docs]}
		return texts, meta
	# score chunks
	scored: List[Tuple[int, Dict[str,str]]] = []
	for ch in chunks:
		text = ch["content"].lower()
		score = sum(text.count(w) for w in words) if words else 0
		# bonus for rate-like tokens to prioritize concrete terms
		if "%" in text:
			score += 5
		if "ставк" in text:
			score += 3
		if "годовы" in text:
			score += 2
		# prefer tariff/financial terms
		if "тариф" in text or "финансов" in text:
			score += 3
		# currency agreement bonus/penalty
		if currency:
			if currency == "RUB" and ("руб" in text or "₽" in text):
				score += 4
			elif currency == "USD" and ("$" in text or "usd" in text or "доллар" in text):
				score += 4
			elif currency == "EUR" and ("€" in text or "eur" in text or "евро" in text):
				score += 4
			elif currency == "CNY" and ("¥" in text or "cny" in text or "юан" in text):
				score += 4
			else:
				score -= 3
		scored.append((score, ch))
	scored.sort(key=lambda x: x[0], reverse=True)
	top_rows = [c for _, c in scored[:limit_chunks]]
	texts = [r["content"] for r in top_rows]
	currs = {r.get("currency") for r in top_rows if r.get("currency")}
	rate_lines: list[str] = []
	for t in texts:
		for rl in _extract_rate_lines(t):
			rate_lines.append(rl)
	# optional trace: store first 200 chars of each chosen chunk
	try:
		if texts:
			preview = [t[:200] for t in texts]
			# We cannot import db here; tracing is handled at call site in get_assistant_reply
			pass
	except Exception:
		pass
	meta = {"currencies": list({c for c in currs if c}), "rates": rate_lines[:10], "via": "keywords", "sources": [{"title": d.get("title",""), "url": d.get("url",""), "id": d.get("id")} for d in (base_docs or [])]}
	return texts, meta



def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	settings = get_settings()
	client = OpenAI(api_key=settings.openai_api_key)

	user_clean = sanitize_text_assistant_output(user_message)
	today = date.today()
	start, end, period_label = _parse_period(user_clean, today)

	# Early off-topic block
	off_topic = _is_off_topic(user_clean)
	if off_topic:
		redirect = _redirect_reply()
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=True)
		db.add_assistant_message(tg_id, "assistant", sanitize_text(redirect), off_topic=False)
		return redirect

	# Period data + plans
	period_stats = db.stats_period(tg_id, start, end)
	plan_info = db.compute_plan_breakdown(tg_id, today)
	# previous period for comparison
	prev_start = start - (end - start) - timedelta(days=1)
	prev_end = start - timedelta(days=1)
	prev_stats = db.stats_period(tg_id, prev_start, prev_end)
	group_rank = db.group_ranking_period(start, end)

	# Direct stats reply with emojis if requested
	if _is_stats_request(user_clean):
		reply = _format_stats_reply(period_label, int(period_stats.get("total", 0)), period_stats.get("by_product", {}), group_rank)
		reply_clean = sanitize_text(reply)
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", reply_clean, off_topic=False)
		return reply_clean

	# Deterministic branch: deposit rates from FACTS (product_rates)
	dep = _try_reply_deposit_rates(db, tg_id, user_clean, today)
	if dep:
		ans = sanitize_text_assistant_output(dep)
		ans = _normalize_bullets(ans)
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", ans, off_topic=False)
		return ans

	# Notes only from employee for context
	notes = db.list_notes_period(tg_id, start, end, limit=3)
	notes_preview = "\n".join([f"{i+1}. {n['content_sanitized']}" for i, n in enumerate(notes)]) if notes else "—"

	# Compose messages for model
	stats_line = (
		f"{period_label}: всего {period_stats['total']}; по продуктам {period_stats['by_product']}; "
		f"план день/неделя/месяц {plan_info['plan_day']}/{plan_info['plan_week']}/{plan_info['plan_month']}; RR {plan_info['rr_month']}"
	)
	prev_line = f"Предыдущий период: всего {prev_stats['total']}; по продуктам {prev_stats['by_product']}"
	best = ", ".join([f"{r['agent_name']}:{r['total']} ]" for r in group_rank[:2]]) if group_rank else "нет данных"
	group_line = f"Лидеры группы за {period_label}: {best}"
	# RAG context (silent for user, no sources in text)
	product_hint = None
	for k in ["КН","кн","кредит налич","наличн","налич","потреб","потребительск","потребительский","потр","наличные"]:
		if k in user_clean.lower():
			product_hint = "КН"
			break
	# deposits
	if not product_hint:
		for k in ["вклад","депозит","депоз" ]:
			if k in user_clean.lower():
				product_hint = "Вклад"
				break
	rag_texts, rag_meta = _rag_top_chunks(db, product_hint, user_clean, limit_docs=3, limit_chunks=5)
	ctx_text = "\n\n".join(rag_texts) if rag_texts else ""
	# Clarify currency if ambiguous
	detected_curr = _detect_currency(user_clean)
	if (not detected_curr) and rag_meta.get("currencies") and len(rag_meta["currencies"]) > 1:
		question = "Уточните валюту вклада: 1) RUB (₽), 2) USD ($), 3) EUR (€), 4) CNY (¥)?"
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", question, off_topic=False)
		return question
	# Guard: не выводим числовые ставки по КН/Вклад, если нет RAG‑строк со ставками
	if product_hint in ("КН","Вклад") and not rag_meta.get("rates"):
		msg = "Чтобы дать точные цифры, уточните, пожалуйста: валюта/тариф/канал. После уточнения пришлю ставки из справки."
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", msg, off_topic=False)
		return msg
	try:
		db.log(tg_id, "rag_ctx", {"count": len(rag_texts) if rag_texts else 0, "previews": [t[:200] for t in (rag_texts or [])], "currencies": rag_meta.get("currencies", []), "via": rag_meta.get("via")})
	except Exception:
		pass

	messages: List[Dict[str, str]] = []
	messages.append({"role": "system", "content": _build_system_prompt(agent_name, stats_line + "; " + prev_line, group_line, notes_preview)})
	# Inject structured FACTS and SOURCES for downstream citation [F#]/[S#], except for auto-summary prompts
	auto_summary = "[auto_summary]" in user_clean.lower()
	if not auto_summary:
		# Compute day/week/month metrics to align FACTS with auto-summary
		try:
			# today
			today_total, _today_by = db._sum_attempts_query(tg_id, today, today)
			p_day = int(plan_info.get('plan_day', 0))
			c_day = (today_total * 100 / p_day) if p_day > 0 else 0
			# meetings and penetration for today
			m_day = db.meets_period_count(tg_id, today, today)
			linked_day = db.attempts_linked_period_count(tg_id, today, today)
			pen_day = (linked_day * 100 / m_day) if m_day > 0 else 0
			facts_lines: List[str] = []
			facts_lines.append(f"F1: Сегодня факт — {today_total}")
			facts_lines.append(f"F2: Сегодня план — {p_day}")
			facts_lines.append(f"F3: Сегодня выполнение, % — {int(round(c_day))}")
			facts_lines.append(f"F4: Сегодня проникновение, % — {int(round(pen_day))}")
			# minimal week/month anchors
			start_week = today - timedelta(days=today.weekday())
			week_total, _ = db._sum_attempts_query(tg_id, start_week, today)
			p_week = int(plan_info.get('plan_week', 0))
			c_week = (week_total * 100 / p_week) if p_week > 0 else 0
			facts_lines.append(f"F5: Неделя факт — {week_total}")
			facts_lines.append(f"F6: Неделя план — {p_week}")
			facts_lines.append(f"F7: Неделя выполнение, % — {int(round(c_week))}")
			start_month = today.replace(day=1)
			month_total, _ = db._sum_attempts_query(tg_id, start_month, today)
			p_month = int(plan_info.get('plan_month', 0))
			c_month = (month_total * 100 / p_month) if p_month > 0 else 0
			facts_lines.append(f"F8: Месяц факт — {month_total}")
			facts_lines.append(f"F9: Месяц план — {p_month}")
			facts_lines.append(f"F10: Месяц выполнение, % — {int(round(c_month))}")
			facts_lines.append(f"F11: RR месяца (прогноз факта) — {int(plan_info.get('rr_month', 0))}")
			sources_lines: List[str] = []
			for i, s in enumerate((rag_meta.get("sources") or [])[:5], start=1):
				title = (s.get("title") or "Источник").strip()
				url = (s.get("url") or "").strip()
				sources_lines.append(f"S{i}: {title} — {url}")
			fs_block = ("FACTS:\n" + "\n".join(facts_lines)) + ("\n\n" + ("SOURCES:\n" + "\n".join(sources_lines)) if sources_lines else "")
			messages.append({"role": "system", "content": fs_block})
		except Exception:
			pass
	if ctx_text:
		# Inject rate lines separately to anchor exact numbers; instruct to cite with [S#]
		rate_block = "\n".join(rag_meta.get("rates", []) or [])
		add = "Справка по продукту (для точности; в ответе используй ссылки [S#] на SOURCES, URL не вставляй напрямую):\n" + ctx_text
		if rate_block:
			add += "\n\nИзвлечённые строки со ставками (используй дословно и всегда указывай валюту):\n" + rate_block
		messages.append({"role": "system", "content": add})
	# Keep last chat history minimal to avoid polluting topic; include last 10
	history = db.get_assistant_messages(tg_id, limit=10)
	for m in history:
		messages.append({"role": m["role"], "content": m["content_sanitized"]})
	messages.append({"role": "user", "content": user_clean})

	resp = client.chat.completions.create(
		model="gpt-4o-mini",
		messages=messages,
		temperature=0.3,
		max_tokens=350,
	)
	answer = resp.choices[0].message.content or ""
	answer_clean = sanitize_text_assistant_output(answer)
	answer_clean = _normalize_bullets(answer_clean)
	answer_clean = _strip_md_emphasis(answer_clean)

	# Store
	db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
	db.add_assistant_message(tg_id, "assistant", answer_clean, off_topic=False)
	return answer_clean 