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
		# Формат
		"Формат ответа по умолчанию (если не просили иначе):\n"
		"1) Сводка (1–2 строки) — что видно и куда двигать.\n"
		"2) Диагностика (2–4 пункта) — что тормозит/что хорошо (по продуктам/этапам).\n"
		"3) Рекомендации (3–6 пунктов) — конкретные шаги/формулировки/фокус‑план.\n"
		"4) План (день/неделя) — SMART‑цели по попыткам/продуктам.\n"
		"5) Контроль — какие метрики посмотреть до следующего контакта.\n"
		# Правила качества
		"Никаких домыслов о тарифах/условиях — говори обобщённо или проси справку. "
		"Пиши строго продукт-специфично: упоминай продукт(ы) из перечня [КН, КСП, ПУ, ДК, ИК, ИЗП, НС, Вклад, КН к ЗП]; если продукт не указан, уточни. "
		"Не делай общих выводов вида ‘скрипт неэффективен’ — укажи конкретный этап и формулировку, которую улучшить. "
		"Привязывай советы к метрикам (attempts, план/факт, RR) и к заметкам сотрудника. Учитывай предыдущую переписку и ранее выданные рекомендации при формулировке новых.\n"
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


def _rag_top_chunks(db: Database, product_hint: Optional[str], query: str, limit_docs: int = 3, limit_chunks: int = 5) -> List[str]:
	"""Pick top chunks from rag_chunks for product. Score by naive keyword hits from query.
	Falls back to rag_docs content if chunks absent.
	"""
	# keywords from query: words >=3 chars
	words = [w for w in re.findall(r"[А-Яа-яA-Za-z0-9%]+", query.lower()) if len(w) >= 3]
	ids: List[str] = []
	try:
		base = _rag_snippets(db, product_hint, limit=limit_docs)
		ids = [r.get("id") for r in base if r.get("id")]
	except Exception:
		ids = []
	chunks: List[Dict[str, str]] = []
	if ids:
		try:
			res = db.client.table("rag_chunks").select("content, chunk_index, product_code, doc_id").in_("doc_id", ids).limit(200).execute()
			rows = getattr(res, "data", []) or []
			for r in rows:
				chunks.append({"content": r.get("content",""), "chunk_index": int(r.get("chunk_index", 0))})
		except Exception:
			chunks = []
	if not chunks:
		# fallback: first 1200 of docs
		docs = _rag_snippets(db, product_hint, limit=limit_docs)
		return [d.get("content","")[:1200] for d in docs if d.get("content")][:limit_chunks]
	# score chunks
	scored: List[Tuple[int, str]] = []
	for ch in chunks:
		text = ch["content"].lower()
		score = sum(text.count(w) for w in words) if words else 0
		scored.append((score, ch["content"]))
	scored.sort(key=lambda x: x[0], reverse=True)
	return [c for _, c in scored[:limit_chunks]]


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	settings = get_settings()
	client = OpenAI(api_key=settings.openai_api_key)

	user_clean = sanitize_text(user_message)
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
	for k in ["КН","кн","кредит налич","наличн","налич" ]:
		if k in user_clean.lower():
			product_hint = "КН"
			break
	# deposits
	if not product_hint:
		for k in ["вклад","депозит","депоз" ]:
			if k in user_clean.lower():
				product_hint = "Вклад"
				break
	rag_texts = _rag_top_chunks(db, product_hint, user_clean, limit_docs=3, limit_chunks=5)
	ctx_text = "\n\n".join(rag_texts) if rag_texts else ""
	try:
		db.log(tg_id, "rag_ctx", {"count": len(rag_texts) if rag_texts else 0})
	except Exception:
		pass

	messages: List[Dict[str, str]] = []
	messages.append({"role": "system", "content": _build_system_prompt(agent_name, stats_line + "; " + prev_line, group_line, notes_preview)})
	if ctx_text:
		messages.append({"role": "system", "content": "Справка по продукту (для точности, не цитируй источники):\n" + ctx_text})
	# Keep last chat history minimal to avoid polluting topic; include last 10
	history = db.get_assistant_messages(tg_id, limit=10)
	for m in history:
		messages.append({"role": m["role"], "content": m["content_sanitized"]})
	messages.append({"role": "user", "content": user_clean})

	resp = client.chat.completions.create(
		model="gpt-4o-mini",
		messages=messages,
		temperature=0.2,
		max_tokens=350,
	)
	answer = resp.choices[0].message.content or ""
	answer_clean = sanitize_text_assistant_output(answer)
	answer_clean = _normalize_bullets(answer_clean)

	# Store
	db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
	db.add_assistant_message(tg_id, "assistant", answer_clean, off_topic=False)
	return answer_clean 