from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Dict

import orjson
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                          KeyboardButton, Message, ReplyKeyboardMarkup)

from .config import get_settings
from .db import Database
from .pii import sanitize_text
from .assistant import get_assistant_reply
from .scheduler import StatsScheduler

PRODUCTS = ["КН","КСП","ПУ","ДК","ИК","ИЗП","НС","Вклад"]


class ResultStates(StatesGroup):
	selecting = State()


class AssistantStates(StatesGroup):
	chatting = State()


@dataclass
class ResultSession:
	counts: Dict[str, int]


def main_keyboard() -> ReplyKeyboardMarkup:
	return ReplyKeyboardMarkup(keyboard=[
		[KeyboardButton(text="Внести результат"), KeyboardButton(text="Статистика")],
		[KeyboardButton(text="Помощник"), KeyboardButton(text="Заметки")],
	], resize_keyboard=True)


def results_keyboard(counts: Dict[str, int]) -> InlineKeyboardMarkup:
	rows = []
	for p in PRODUCTS:
		c = counts.get(p, 0)
		rows.append([InlineKeyboardButton(text=f"{p} [{c}] ➕", callback_data=f"inc:{p}"), InlineKeyboardButton(text="➖", callback_data=f"dec:{p}")])
	rows.append([InlineKeyboardButton(text="Готово", callback_data="done"), InlineKeyboardButton(text="Отмена", callback_data="cancel")])
	return InlineKeyboardMarkup(inline_keyboard=rows)


async def notify(bot: Bot, chat_id: int, text: str) -> None:
	await bot.send_message(chat_id, text)


async def run_bot() -> None:
	settings = get_settings()
	bot = Bot(token=settings.telegram_bot_token, parse_mode=ParseMode.HTML)
	dp = Dispatcher(storage=MemoryStorage())
	db = Database()
	await asyncio.to_thread(db.ensure_allowed_users_bootstrap, settings.allowed_tg_ids_bootstrap)

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

	# Text buttons
	@dp.message(F.text == "Внести результат")
	@dp.message(Command("result"))
	async def enter_results(message: Message, state: FSMContext) -> None:
		user_id = message.from_user.id
		if not db.is_allowed(user_id):
			await message.answer("Доступ ограничен.")
			return
		# Ensure employee exists
		emp = db.get_or_register_employee(user_id)
		if not emp:
			await message.answer("Временная ошибка базы. Повторите позже.", reply_markup=main_keyboard())
			return
		await state.set_state(ResultStates.selecting)
		await state.update_data(session=ResultSession(counts={}).__dict__)
		await message.answer("Выберите продукты и количество попыток", reply_markup=results_keyboard({}))

	@dp.callback_query(ResultStates.selecting, F.data.startswith("inc:"))
	async def inc_count(call: CallbackQuery, state: FSMContext) -> None:
		data = await state.get_data()
		session = ResultSession(**data.get("session"))
		p = call.data.split(":",1)[1]
		session.counts[p] = session.counts.get(p, 0) + 1
		await state.update_data(session=session.__dict__)
		await call.message.edit_reply_markup(reply_markup=results_keyboard(session.counts))
		await call.answer()

	@dp.callback_query(ResultStates.selecting, F.data.startswith("dec:"))
	async def dec_count(call: CallbackQuery, state: FSMContext) -> None:
		data = await state.get_data()
		session = ResultSession(**data.get("session"))
		p = call.data.split(":",1)[1]
		current = session.counts.get(p, 0)
		if current > 0:
			session.counts[p] = current - 1
		await state.update_data(session=session.__dict__)
		await call.message.edit_reply_markup(reply_markup=results_keyboard(session.counts))
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
		session = ResultSession(**data.get("session"))
		try:
			# Ensure employee exists before saving
			emp = db.get_or_register_employee(call.from_user.id)
			if not emp:
				raise RuntimeError("employee missing")
			db.save_attempts(call.from_user.id, session.counts, date.today())
			db.log(call.from_user.id, "save_attempts", session.counts)
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
		top_str = ", ".join([r["agent_name"] for r in top2]) if top2 else "—"
		bottom_str = ", ".join([r["agent_name"] for r in bottom2]) if bottom2 else "—"
		lines = [
			f"1. Агент: {emp.agent_name} — место за месяц: {pos if pos else '—'} 🏆",
			f"2. Сегодня: {stats['today']['total']} 🎯",
			f"3. Неделя: {stats['week']['total']} 📅",
			f"4. Месяц: {stats['month']['total']} 📊",
			f"5. Топ-2 сегодня: {top_str} 🥇",
			f"6. Антилидеры: {bottom_str} 🧱",
		]
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
		# Telegram hard limit ~4096 chars; split into safe chunks
		MAX_LEN = 3500
		if len(reply) <= MAX_LEN:
			await message.answer(reply, reply_markup=main_keyboard())
		else:
			start = 0
			while start < len(reply):
				chunk = reply[start:start+MAX_LEN]
				await message.answer(chunk)
				start += MAX_LEN
			await message.answer("(конец ответа)", reply_markup=main_keyboard())

	# Native command menu
	try:
		await bot.set_my_commands([
			BotCommand(command="menu", description="Показать меню"),
		])
	except Exception:
		pass

	# Start scheduler for daily summary
	async def push(chat_id: int, text: str) -> None:
		await bot.send_message(chat_id, text)
	StatsScheduler(db, push).start()

	# Ensure webhook is disabled for polling
	try:
		await bot.delete_webhook(drop_pending_updates=True)
	except Exception:
		pass

	print("Bot is running...")
	await dp.start_polling(bot)


if __name__ == "__main__":
	asyncio.run(run_bot()) 