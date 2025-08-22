from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Set

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                          KeyboardButton, Message, ReplyKeyboardMarkup)

from .db import Database
from .pii import sanitize_text
from .assistant import get_assistant_reply

PRODUCTS = ["КН","КСП","ПУ","ДК","ИК","ИЗП","НС","Вклад"]


class ResultStates(StatesGroup):
	selecting = State()


class AssistantStates(StatesGroup):
	chatting = State()


@dataclass
class ResultSession:
	selected: Set[str]


def main_keyboard() -> ReplyKeyboardMarkup:
	return ReplyKeyboardMarkup(keyboard=[
		[KeyboardButton(text="Внести результат"), KeyboardButton(text="Статистика")],
		[KeyboardButton(text="Помощник"), KeyboardButton(text="Заметки")],
	], resize_keyboard=True)


def results_keyboard(selected: Set[str]) -> InlineKeyboardMarkup:
	rows = []
	for p in PRODUCTS:
		mark = "✅" if p in selected else "⬜️"
		rows.append([InlineKeyboardButton(text=f"{mark} {p}", callback_data=f"toggle:{p}")])
	rows.append([InlineKeyboardButton(text="Готово", callback_data="done"), InlineKeyboardButton(text="Отмена", callback_data="cancel")])
	return InlineKeyboardMarkup(inline_keyboard=rows)


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

	@dp.message(F.text == "Внести результат")
	@dp.message(Command("result"))
	async def enter_results(message: Message, state: FSMContext) -> None:
		user_id = message.from_user.id
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
		try:
			emp = db.get_or_register_employee(call.from_user.id)
			if not emp:
				raise RuntimeError("employee missing")
			attempts = {p: 1 for p in selected}
			db.save_attempts(call.from_user.id, attempts, date.today())
			db.log(call.from_user.id, "save_attempts", attempts)
			try:
				await call.message.edit_text("Результат сохранен")
			except Exception:
				await call.message.answer("Результат сохранен")
		except Exception as e:
			db.log(call.from_user.id, "error", {"where": "done_results", "error": str(e)})
			try:
				await call.message.edit_text("Ошибка сохранения. Повторите позже.")
			except Exception:
				await call.message.answer("Ошибка сохранения. Повторите позже.")
		finally:
			await state.clear()
			stats = db.stats_day_week_month(call.from_user.id, date.today())
			await call.message.answer(
				f"День: {stats['today']['total']} | Неделя: {stats['week']['total']} | Месяц: {stats['month']['total']}",
				reply_markup=main_keyboard(),
			)
			await call.answer()

	@dp.message(F.text == "Статистика")
	@dp.message(Command("stats"))
	async def stats_handler(message: Message) -> None:
		user_id = message.from_user.id
		if not db.is_allowed(user_id):
			await message.answer("Доступ ограничен.")
			return
		emp = db.get_or_register_employee(user_id)
		if not emp:
			await message.answer("Временная ошибка базы. Повторите позже.")
			return
		today = date.today()
		stats = db.stats_day_week_month(user_id, today)
		month_rank = db.month_ranking(today.replace(day=1), today)
		pos = next((i+1 for i, r in enumerate(month_rank) if r["tg_id"] == user_id), None)
		top2, bottom2 = db.day_top_bottom(today)
		lines = [
			f"Вы: {emp.agent_name} — место в рейтинге за месяц: {pos if pos else '—'}",
			f"День: {stats['today']['total']}",
			f"Неделя: {stats['week']['total']}",
			f"Месяц: {stats['month']['total']}",
			"Топ-2 сегодня:" if top2 else "Топ-2 сегодня: —",
		]
		for r in top2:
			lines.append(f"{r['agent_name']}")
		lines.append("Худшие-2 сегодня:" if bottom2 else "Худшие-2 сегодня: —")
		for r in bottom2:
			lines.append(f"{r['agent_name']}")
		await message.answer("\n".join(lines), reply_markup=main_keyboard())

	@dp.message(F.text == "Заметки")
	@dp.message(Command("notes"))
	async def notes_menu(message: Message) -> None:
		kb = InlineKeyboardMarkup(inline_keyboard=[
			[InlineKeyboardButton(text="Внести комментарий", callback_data="note:add")],
			[InlineKeyboardButton(text="Мои комментарии", callback_data="note:list")],
		])
		await message.answer("Заметки:", reply_markup=kb)

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
			text = "\n\n".join([f"{n['created_at']}:\n{n['content_sanitized']}" for n in notes])
			await call.message.answer(text)
		await call.answer()

	@dp.message(F.text == "Помощник")
	@dp.message(Command("assistant"))
	async def assistant_start(message: Message, state: FSMContext) -> None:
		await state.set_state(AssistantStates.chatting)
		await state.update_data(mode="assistant")
		await message.answer("Я готов помочь. Напишите вопрос. /cancel для выхода")

	@dp.message(F.text == "/cancel")
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
		emp = db.get_or_register_employee(message.from_user.id)
		if not emp:
			await message.answer("Временная ошибка базы. Повторите позже.", reply_markup=main_keyboard())
			return
		today = date.today()
		stats = db.stats_day_week_month(message.from_user.id, today)
		month_rank = db.month_ranking(today.replace(day=1), today)
		reply = get_assistant_reply(db, message.from_user.id, emp.agent_name, stats, month_rank, message.text or "")
		await message.answer(reply, reply_markup=main_keyboard()) 