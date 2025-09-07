from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, Set, List, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                         KeyboardButton, Message, ReplyKeyboardMarkup)

from .db import Database
from .pii import sanitize_text
from .assistant import get_assistant_reply

PRODUCTS: List[str] = ["КН","КСП","ПУ","ДК","ИК","ИЗП","НС","Вклад","КН к ЗП"]
DELIVERY_PRODUCTS: List[str] = ["ЗП","ДК","МК","ПУ","КН","ТС","Вклад","ИК","Эскроу","КК","Аккредитив"]


class ResultStates(StatesGroup):
	selecting = State()


class MeetStates(StatesGroup):
	selecting = State()


class AssistantStates(StatesGroup):
	chatting = State()


@dataclass
class ResultSession:
	selected: Set[str]


@dataclass
class MeetSession:
	product: Optional[str]


def main_keyboard() -> ReplyKeyboardMarkup:
	return ReplyKeyboardMarkup(keyboard=[
		[KeyboardButton(text="Внести кросс"), KeyboardButton(text="Статистика")],
		[KeyboardButton(text="Внести встречу"), KeyboardButton(text="Заметки")],
		[KeyboardButton(text="Помощник")],
	], resize_keyboard=True)


def _label(p: str, selected: Set[str]) -> str:
	mark = "✅" if p in selected else "⬜️"
	return f"{mark} {p}"


def results_keyboard(selected: Set[str]) -> InlineKeyboardMarkup:
	# 2 колонки → шире кнопки
	buttons: List[List[InlineKeyboardButton]] = []
	row: List[InlineKeyboardButton] = []
	for idx, p in enumerate(PRODUCTS):
		row.append(InlineKeyboardButton(text=_label(p, selected), callback_data=f"toggle:{p}"))
		if (idx + 1) % 2 == 0:
			buttons.append(row)
			row = []
	if row:
		buttons.append(row)
	buttons.append([InlineKeyboardButton(text="Готово", callback_data="done"), InlineKeyboardButton(text="Отмена", callback_data="cancel")])
	return InlineKeyboardMarkup(inline_keyboard=buttons)


def meet_keyboard(selected: Optional[str]) -> InlineKeyboardMarkup:
	buttons: List[List[InlineKeyboardButton]] = []
	row: List[InlineKeyboardButton] = []
	for idx, p in enumerate(DELIVERY_PRODUCTS):
		mark = "✅ " if selected == p else ""
		row.append(InlineKeyboardButton(text=f"{mark}{p}", callback_data=f"meet:{p}"))
		if (idx + 1) % 2 == 0:
			buttons.append(row)
			row = []
	if row:
		buttons.append(row)
	buttons.append([
		InlineKeyboardButton(text="Готово", callback_data="meet_done"),
		InlineKeyboardButton(text="Отмена", callback_data="meet_cancel"),
		InlineKeyboardButton(text="Внести кросс", callback_data="meet_cross"),
	])
	return InlineKeyboardMarkup(inline_keyboard=buttons)


def _fmt1(val: float | int) -> str:
	"""Format with at most one decimal and comma as separator.
	- If whole number → without decimals (e.g., 30)
	- Else → one decimal (e.g., 29,3)
	"""
	try:
		v = float(val)
		if abs(v - round(v)) < 1e-9:
			return str(int(round(v)))
		return f"{v:.1f}".replace('.', ',')
	except Exception:
		return str(val)


def register_handlers(dp: Dispatcher, db: Database, bot: Bot, *, for_webhook: bool = False) -> None:
	@dp.message(CommandStart())
	async def start_handler(message: Message) -> None:
		user_id = message.from_user.id
		if not db.is_allowed(user_id):
			await message.answer("Доступ ограничен. Ваш ID не в списке.")
			return
		emp = db.get_or_register_employee(user_id)
		if not emp:
			db.log(user_id, "error", {"where": "start_handler", "msg": "employee None"})
			await message.answer("Временная ошибка базы. Повторите позже.")
			return
		db.log(user_id, "start", {"username": message.from_user.username})
		await message.answer(f"Привет, {emp.agent_name}!", reply_markup=main_keyboard())

	@dp.message(Command("menu"))
	async def menu_handler(message: Message) -> None:
		await message.answer("Меню", reply_markup=main_keyboard())

	# Cross attempts flow
	@dp.message(F.text == "Внести кросс")
	@dp.message(Command("result"))
	@dp.message(lambda m: (m.text or "").strip().lower() == "внести кросс")
	async def enter_results(message: Message, state: FSMContext) -> None:
		user_id = message.from_user.id
		# Access уже проверен ранее, но оставим мягкую проверку
		if not db.is_allowed(user_id):
			await message.answer("Доступ ограничен.")
			return
		emp = db.get_or_register_employee(user_id)
		if not emp:
			await message.answer("Временная ошибка базы. Повторите позже.", reply_markup=main_keyboard())
			return
		await state.set_state(ResultStates.selecting)
		await state.update_data(session=ResultSession(selected=set()).__dict__)
		await message.answer("Отметьте продукты (чек-боксы)", reply_markup=results_keyboard(set()))

	@dp.callback_query(ResultStates.selecting, F.data.startswith("toggle:"))
	async def toggle_product(call: CallbackQuery, state: FSMContext) -> None:
		data = await state.get_data()
		session = ResultSession(set(data.get("session", {}).get("selected", [])))
		p = call.data.split(":",1)[1]
		if p in session.selected:
			session.selected.remove(p)
		else:
			session.selected.add(p)
		await state.update_data(session={"selected": list(session.selected)})
		await call.message.edit_reply_markup(reply_markup=results_keyboard(session.selected))
		await call.answer()

	@dp.callback_query(ResultStates.selecting, F.data == "cancel")
	async def cancel_results(call: CallbackQuery, state: FSMContext) -> None:
		await state.clear()
		try:
			await call.message.edit_text("Результат не сохранен")
		except Exception:
			await call.message.answer("Результат не сохранен")
		stats = db.stats_day_week_month(call.from_user.id, date.today())
		await call.message.answer(
			f"День: {stats['today']['total']} | Неделя: {stats['week']['total']} | Месяц: {stats['month']['total']}",
			reply_markup=main_keyboard(),
		)
		await call.answer()

	@dp.callback_query(ResultStates.selecting, F.data == "done")
	async def done_results(call: CallbackQuery, state: FSMContext) -> None:
		data = await state.get_data()
		selected = set(data.get("session", {}).get("selected", []))
		meet_id = data.get("meet_id")
		try:
			emp = db.get_or_register_employee(call.from_user.id)
			if not emp:
				raise RuntimeError("employee missing")
			attempts = {p: 1 for p in selected}
			db.save_attempts(call.from_user.id, attempts, date.today(), meet_id=meet_id)
			db.log(call.from_user.id, "save_attempts", attempts)
			try:
				await call.message.edit_text("Результат сохранен")
			except Exception:
				await call.message.answer("Результат сохранен")
			# Post-save summary: cross totals and optionally meetings totals
			today = date.today()
			# Cross totals
			stats = db.stats_day_week_month(call.from_user.id, today)
			cross_line = (
				f"Кросс: День {int(stats['today']['total'])} | Неделя {int(stats['week']['total'])} | Месяц {int(stats['month']['total'])}"
			)
			await call.message.answer(cross_line)
			# If came from /meet (we carry meet_id), also show meetings line
			if meet_id:
				start_week = today - timedelta(days=today.weekday())
				start_month = today.replace(day=1)
				m_day = db.meets_period_count(call.from_user.id, today, today)
				m_week = db.meets_period_count(call.from_user.id, start_week, today)
				m_month = db.meets_period_count(call.from_user.id, start_month, today)
				meet_line = f"Встречи: День {m_day} | Неделя {m_week} | Месяц {m_month}"
				await call.message.answer(meet_line)
		except Exception as e:
			db.log(call.from_user.id, "error", {"where": "done_results", "error": str(e)})
			try:
				await call.message.edit_text("Ошибка сохранения. Повторите позже.")
			except Exception:
				await call.message.answer("Ошибка сохранения. Повторите позже.")
		finally:
			await state.clear()
			await call.message.answer("Меню", reply_markup=main_keyboard())
			await call.answer()

	# Meet flow
	@dp.message(F.text == "Внести встречу")
	@dp.message(Command("meet"))
	@dp.message(lambda m: (m.text or "").strip().lower() == "внести встречу")
	async def meet_start(message: Message, state: FSMContext) -> None:
		await message.answer("Функция временно недоступна", reply_markup=main_keyboard())

	@dp.callback_query(MeetStates.selecting, F.data.startswith("meet:"))
	async def meet_pick(call: CallbackQuery, state: FSMContext) -> None:
		p = call.data.split(":",1)[1]
		data = await state.get_data()
		sess = MeetSession(**data.get("meet"))
		sess.product = p
		await state.update_data(meet=sess.__dict__)
		await call.message.edit_reply_markup(reply_markup=meet_keyboard(sess.product))
		await call.answer()

	@dp.callback_query(MeetStates.selecting, F.data == "meet_cancel")
	async def meet_cancel(call: CallbackQuery, state: FSMContext) -> None:
		await state.clear()
		try:
			await call.message.edit_text("Результат не сохранен")
		except Exception:
			await call.message.answer("Результат не сохранен")
		await call.answer()

	@dp.callback_query(MeetStates.selecting, F.data == "meet_cross")
	async def meet_to_cross(call: CallbackQuery, state: FSMContext) -> None:
		user_id = call.from_user.id
		if not db.is_allowed(user_id):
			await call.message.answer("Доступ ограничен.")
			await call.answer()
			return
		# Start cross flow explicitly for the user (message.from_user in callbacks is the bot)
		emp = db.get_or_register_employee(user_id)
		if not emp:
			await call.message.answer("Временная ошибка базы. Повторите позже.", reply_markup=main_keyboard())
			await call.answer()
			return
		# Create meet immediately (use chosen delivery product if selected)
		data = await state.get_data()
		sess = data.get("meet") or {}
		prod = sess.get("product") or "—"
		meet_id = db.create_meet(user_id, prod, date.today())
		await state.clear()
		await state.set_state(ResultStates.selecting)
		await state.update_data(session=ResultSession(selected=set()).__dict__, meet_id=meet_id)
		await bot.send_message(user_id, "Отметьте продукты (чек-боксы)", reply_markup=results_keyboard(set()))
		await call.answer()

	@dp.callback_query(MeetStates.selecting, F.data == "meet_done")
	async def meet_done(call: CallbackQuery, state: FSMContext) -> None:
		data = await state.get_data()
		sess = MeetSession(**data.get("meet"))
		if not sess.product:
			await call.answer("Выберите продукт", show_alert=True)
			return
		try:
			meet_id = db.create_meet(call.from_user.id, sess.product, date.today())
			if not meet_id:
				raise RuntimeError("meet not created")
			# Log already inside create_meet; also reflect to logs table explicitly
			try:
				db.log(call.from_user.id, "meet_saved", {"meet_id": meet_id, "product": sess.product})
			except Exception:
				pass
			try:
				await call.message.edit_text("Результат сохранен")
			except Exception:
				await call.message.answer("Результат сохранен")
			# Post-save summary: meetings totals
			today = date.today()
			start_week = today - timedelta(days=today.weekday())
			start_month = today.replace(day=1)
			m_day = db.meets_period_count(call.from_user.id, today, today)
			m_week = db.meets_period_count(call.from_user.id, start_week, today)
			m_month = db.meets_period_count(call.from_user.id, start_month, today)
			meet_line = f"Встречи: День {m_day} | Неделя {m_week} | Месяц {m_month}"
			await call.message.answer(meet_line)
		except Exception as e:
			db.log(call.from_user.id, "error", {"where": "meet_done", "error": str(e)})
			try:
				await call.message.edit_text("Ошибка сохранения. Повторите позже.")
			except Exception:
				await call.message.answer("Ошибка сохранения. Повторите позже.")
		finally:
			await state.clear()
			await call.answer()

	@dp.message(F.text == "Статистика")
	@dp.message(Command("stats"))
	@dp.message(lambda m: (m.text or "").strip().lower() == "статистика")
	async def stats_handler(message: Message) -> None:
		await message.answer("Функция временно недоступна", reply_markup=main_keyboard())

	# Notes and Assistant handlers below remain unchanged
	@dp.message(F.text == "Заметки")
	@dp.message(Command("notes"))
	@dp.message(lambda m: (m.text or "").strip().lower() == "заметки")
	async def notes_menu(message: Message) -> None:
		await message.answer("Функция временно недоступна", reply_markup=main_keyboard())

	@dp.callback_query(F.data == "note:add")
	async def note_add_start(call: CallbackQuery, state: FSMContext) -> None:
		await state.set_state(AssistantStates.chatting)
		await state.update_data(mode="note_add")
		await call.message.answer("Введите комментарий. /cancel для отмены")
		await call.answer()

	@dp.callback_query(F.data == "note:list")
	async def note_list(call: CallbackQuery) -> None:
		notes = db.list_notes(call.from_user.id, limit=20)
		if not notes:
			await call.message.answer("Комментариев нет")
		else:
			def _d(v: object) -> str:
				s = str(v)
				try:
					dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
					return dt.strftime('%d.%m.%Y')
				except Exception:
					return s.split('T', 1)[0]
			text = "\n\n".join([f"{_d(n['created_at'])}:\n{n['content_sanitized']}" for n in notes])
			await call.message.answer(text)
		await call.answer()

	@dp.message(F.text == "Помощник")
	@dp.message(Command("assistant"))
	@dp.message(lambda m: (m.text or "").strip().lower() == "помощник")
	async def assistant_start(message: Message, state: FSMContext) -> None:
		await state.set_state(AssistantStates.chatting)
		await state.update_data(mode="assistant")
		await message.answer("Я готов помочь. Напишите вопрос. /cancel для выхода")

	@dp.message(F.text == "/cancel")
	@dp.message(lambda m: (m.text or "").strip().lower() == "отменено" or (m.text or "").strip().lower() == "отмена")
	async def cancel_any(message: Message, state: FSMContext) -> None:
		await state.clear()
		await message.answer("Отменено", reply_markup=main_keyboard())

	@dp.message(AssistantStates.chatting)
	async def assistant_or_note(message: Message, state: FSMContext) -> None:
		data = await state.get_data()
		mode = data.get("mode")
		if mode == "note_add":
			text = sanitize_text(message.text or "")
			if not text:
				await message.answer("Пусто. Отменено.", reply_markup=main_keyboard())
			else:
				db.add_note(message.from_user.id, text)
				db.log(message.from_user.id, "note_add", {"len": len(text)})
				await message.answer("Результат сохранен", reply_markup=main_keyboard())
			await state.clear()
			return
		# assistant
		try:
			db.log(message.from_user.id, "assistant_in", {"text": (message.text or "")[:300]})
		except Exception:
			pass
		emp = db.get_or_register_employee(message.from_user.id)
		if not emp:
			await message.answer("Временная ошибка базы. Повторите позже.", reply_markup=main_keyboard())
			return
		today = date.today()
		try:
			stats = db.stats_day_week_month(message.from_user.id, today)
			month_rank = db.month_ranking(today.replace(day=1), today)
			reply = get_assistant_reply(db, message.from_user.id, emp.agent_name, stats, month_rank, message.text or "")
			await message.answer(reply, reply_markup=main_keyboard())
		except Exception as e:
			try:
				db.log(message.from_user.id, "assistant_error", {"error": str(e)})
			except Exception:
				pass
			await message.answer("Техническая пауза. Повторите запрос одним сообщением.", reply_markup=main_keyboard())

	# FINAL CATCH-ALL: forward any unmatched text into assistant mode
	@dp.message()
	async def catch_all(message: Message, state: FSMContext) -> None:
		# If not allowed, short-circuit
		if not db.is_allowed(message.from_user.id):
			return
		# Ensure assistant mode and forward
		await state.set_state(AssistantStates.chatting)
		await state.update_data(mode="assistant")
		# Reuse assistant handler path
		try:
			db.log(message.from_user.id, "assistant_forward", {"text": (message.text or "")[:200]})
		except Exception:
			pass
		# Call get_assistant_reply directly
		emp = db.get_or_register_employee(message.from_user.id)
		if not emp:
			await message.answer("Временная ошибка базы. Повторите позже.", reply_markup=main_keyboard())
			return
		today = date.today()
		try:
			stats = db.stats_day_week_month(message.from_user.id, today)
			month_rank = db.month_ranking(today.replace(day=1), today)
			reply = get_assistant_reply(db, message.from_user.id, emp.agent_name, stats, month_rank, message.text or "")
			await message.answer(reply, reply_markup=main_keyboard())
		except Exception as e:
			try:
				db.log(message.from_user.id, "assistant_error_final", {"error": str(e)})
			except Exception:
				pass
			await message.answer("Техническая пауза. Повторите запрос одним сообщением.", reply_markup=main_keyboard()) 