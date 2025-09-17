from __future__ import annotations

from typing import Any, Dict, List

from openai import OpenAI
import re

from .db import Database, is_workday
from .pii import sanitize_text_assistant_output
from .config import get_settings
from datetime import datetime
import pytz


def _normalize_output(text: str) -> str:
	"""Remove markdown '**' and ensure bullets/numbered items start on new lines."""
	if not text:
		return ""
	# drop bold markers
	clean = text.replace("**", "")
	# normalize newlines
	clean = clean.replace("\r\n", "\n").replace("\r", "\n")
	# insert newline before N) bullets if not at start of line
	clean = re.sub(r"(?<!^)\s+(?=\d{1,2}\)\s)", "\n", clean)
	# insert newline before N. bullets if not at start of line
	clean = re.sub(r"(?<!^)\s+(?=\d{1,2}\.\s)", "\n", clean)
	# insert newline before hyphen bullets if not at start of line
	clean = re.sub(r"(?<!^)\s+(?=-\s)", "\n", clean)
	# collapse excessive blank lines
	clean = re.sub(r"\n{3,}", "\n\n", clean)
	return clean.strip()


def get_assistant_reply(db: Database, tg_id: int, agent_name: str, user_stats: Dict[str, Any], group_month_ranking: List[Dict[str, Any]], user_message: str) -> str:
	"""Normal dialog: route user message to OpenAI with a concise RU system prompt.
	Adds the last 10 messages of the dialog to preserve context.
	"""
	settings = get_settings()
	user_clean = sanitize_text_assistant_output(user_message)
	# Build messages with short system prompt
	system_prompt = (
		"Ты - менеджер по развитию бизнеса, твоя задача помогать сотрудника банка осуществлять кросс-продажи продуктов банка на встречах с клиентами, помогать сотрудникам выполнять план продаж\n\n"
		"Сотрудники банка - менеджеры, которые выезжают к клиенту по заранее оформленным заявкам на встречи с целью оформления продукта банка и кросс-продажи дополнительных продуктов.\n\n"
		"Роль\n"
		"- эксперт по продажам розничных продуктов/услуг крупного российского банка \n"
		"- мастер вежливого общения\n"
		"- коуч и наставник для сотрудников банка\n\n"
		"Инструкции\n"
		"- на вопросы отвечай максимально конкретно \n"
		"- не используй никаких персональных данных при ответах\n"
		"- не веди диалог на посторонние темы, если получишь сообщение не по теме, вежливо уточни, что общаешься только по продажам продуктов и услуг банка\n"
        "- не отвечай на вопросы о суммах, сроках, ставках и прочей точной информации, которая зависит от конкретной организации, вежливой рекомендуй сотруднику уточнить информацию в раздаточных метериалах\n"
        "- если сотрудник сам указывает в сообщении точную информацию (суммы, сроки, ставки и тд) - можешь ей оперировать в диалоге\n"
        "- не противореч сам себе в ответах\n"
        "- не давай рекомендаций направленных на увеличение количества встреч - сотрудник доставки на это не влияет\n"
		"- используй информацию о сокращениях из Продукты банка для кросс продаж и Продукты банка для оформления при диалоге с сотрудником\n\n"
		"Стиль ответа\n"
		"- используй эмодзи для эмоцинального окраса (1-2 в сообщении)\n"
		"- подчеркивай выгоды и преимущества предлагаемых решений\n"
		"- предлагай дополнительные варианты решения вопросов\n"
		"- ставь цели в формате SMART\n"
		"- в вежливой форме требуй от сотрудников выполнения поставленных целей\n"
        "- подкрепляй ответы расчетами и цифрами если это уместно и это не противоречит предыдущим инструкциям\n"
		"- общаешься на \"Ты\"\n\n"
		"Пример ответа\n"
		"- Для увеличения процента прониновения продаж во встречи попробуй выявить потребность задав не менее 3х уточняющих вопросов\n"
		"- Текущий уровень проникновения продаж во встречи не позволяет выйти тебе на плановое значение к концу месяца, продай не менее <ответ на основании вычислений> продуктов в течение следующей недели\n\n"
		"Продукты банка для кросс продаж\n"
		"- КН - кредит наличными\n"
		"- КК - кредитная карта\n"
		"- Вклад - вклад\n"
		"- НС - накопительный счет\n"
		"- КСП - комиссионнно-страховые продукты\n"
		"- ИК - ипотечный кредит\n"
		"- ИЗП - дебетовая зарплатная карта в рамках индивидуального зарплатного проекта \n"
		"- ДК - дебетовая карта\n"
		"- ПУ - пакет услуг Премиальный\n\n"
		"Продукты банка для оформления\n"
		"- ЗП - карта в рамках зарплатного проекта\n"
		"- ДК - дебетовая именная карта\n"
		"- МК - дебетовая неименная карта\n"
		"- ПУ - пакет услуг Премиальный\n"
		"- КН - кредит наличными\n"
		"- ТС - текущий счет\n"
		"- Вклад - вклад\n"
		"- ИК - ипотечный кредит\n"
		"- Эскроу - эскроу\n"
		"- КК - кредитная карта\n"
		"- Аккредитив - аккредитив"
	)
	# calendar context
	tz = pytz.timezone(settings.timezone)
	now = datetime.now(tz)
	d = now.date()
	weekday = ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"][d.weekday()]
	workflag = "рабочий" if is_workday(d) else "выходной"
	calendar_info = f"Календарь: сегодня {d.isoformat()} ({weekday}), {workflag}; время {now.strftime('%H:%M')} {settings.timezone}"
	messages: List[Dict[str, str]] = [
		{"role": "system", "content": system_prompt},
		{"role": "system", "content": calendar_info},
	]
	# Pull last 10 messages from history and add to context (role, content)
	try:
		history = db.get_assistant_messages(tg_id, limit=10)
		for m in history:
			role = m.get("role") or "user"
			content = m.get("content_sanitized") or ""
			if role not in ("user", "assistant", "system"):
				role = "user"
			messages.append({"role": role, "content": content})
	except Exception:
		pass
	# Append current user message
	messages.append({"role": "user", "content": user_clean})
	answer = ""
	 try:
                client = OpenAI(api_key=settings.openai_api_key)
                resp = client.chat.completions.create(
                model=settings.assistant_model,
                messages=messages,
                        temperature=0.5,
                        max_tokens=400,
        )
                answer = resp.choices[0].message.content or ""
        except Exception as exc:
                error_text = str(exc)
                try:
                        db.log(tg_id, "assistant_openai_error", {"error": error_text})
                except Exception:
                        pass
                print(f"assistant_openai_error: {error_text}", flush=True)
                raise
	answer_clean = sanitize_text_assistant_output(answer)
	answer_clean = _normalize_output(answer_clean)
	try:
		db.add_assistant_message(tg_id, "user", user_clean, off_topic=False)
		db.add_assistant_message(tg_id, "assistant", answer_clean, off_topic=False)
	except Exception:
		pass
	return answer_clean 
