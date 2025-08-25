from __future__ import annotations

from typing import Any, Dict, List, Tuple
from datetime import date, datetime, timedelta

from openai import OpenAI

from .config import get_settings
from .pii import sanitize_text, sanitize_text_assistant_output
from .db import Database
import re


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
	"""Ensure that numbered bullets '1.', '2.' start on new lines.
	- Inserts a newline before any occurrence of '<digits>. ' that is not already at line start.
	- Collapses extra spaces around newlines.
	"""
	if not text:
		return ""
	# Insert newline before N. where N=1..99 if not already at start of line
	normalized = re.sub(r"(?<!^)\s+(?=\d{1,2}\.\s)", "\n", text)
	# Ensure Windows/Mac newlines normalized
	normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
	# Trim trailing spaces per line
	normalized = "\n".join(line.rstrip() for line in normalized.split("\n"))
	return normalized.strip()


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
	best = ", ".join([f"{r['agent_name']}:{r['total']}]" for r in group_rank[:2]]) if group_rank else "нет данных"
	group_line = f"Лидеры группы за {period_label}: {best}"
	messages: List[Dict[str, str]] = []
	messages.append({"role": "system", "content": _build_system_prompt(agent_name, stats_line + "; " + prev_line, group_line, notes_preview)})
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