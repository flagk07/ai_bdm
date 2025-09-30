from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
from typing import Any, Dict, Set, List, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                         KeyboardButton, Message, ReplyKeyboardMarkup)

from .db import Database
from .config import get_settings
from .pii import sanitize_text
from .assistant import get_assistant_reply

PRODUCTS: List[str] = ["КН","КСП 4000+","КСП 1600","ПДС","ПУ","ДК","ИК","ИЗП","НС","Вклад","КН к ЗП"]
DELIVERY_PRODUCTS: List[str] = ["ЗП","ДК","МК","ПУ","КН","ТС","Вклад","ИК","Эскроу","КК","Аккредитив"]


class ResultStates(StatesGroup):
	selecting = State()


class MeetStates(StatesGroup):
	selecting = State()


class AssistantStates(StatesGroup):
	chatting = State()


class MassIssueStates(StatesGroup):
	entering = State()
	cross_selecting = State()
	cross_wait_count = State()


@dataclass
class ResultSession:
	selected: Set[str]


@dataclass
class MeetSession:
	product: Optional[str]


def _kb_work_open() -> ReplyKeyboardMarkup:
	return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Начать работу")]], resize_keyboard=True)


def main_keyboard() -> ReplyKeyboardMarkup:
	# Full keyboard when work is open
	return ReplyKeyboardMarkup(keyboard=[
		[KeyboardButton(text="Статистика"), KeyboardButton(text="Заметки")],
		[KeyboardButton(text="Внести встречу"), KeyboardButton(text="Помощник")],
		[KeyboardButton(text="Массовая выдача"), KeyboardButton(text="Завершить работу")],
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
		# Gate by work session
		if db.work_is_open(user_id):
			await message.answer(f"Привет, {emp.agent_name}!", reply_markup=main_keyboard())
		else:
			await message.answer(f"Привет, {emp.agent_name}! Нажми 'Начать работу' чтобы активировать функции.", reply_markup=_kb_work_open())

	@dp.message(Command("menu"))
	async def menu_handler(message: Message) -> None:
		if db.work_is_open(message.from_user.id):
			await message.answer("Меню", reply_markup=main_keyboard())
		else:
			await message.answer("Меню", reply_markup=_kb_work_open())

	# ===== Массовая выдача =====

	def _kb_mass_issue_post_count() -> InlineKeyboardMarkup:
		return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Готово", callback_data="mi:done"), InlineKeyboardButton(text="Отмена", callback_data="mi:cancel"), InlineKeyboardButton(text="Внести кросс", callback_data="mi:cross")]])

	def _kb_mass_cross(selected: Dict[str, int]) -> InlineKeyboardMarkup:
		buttons: List[List[InlineKeyboardButton]] = []
		row: List[InlineKeyboardButton] = []
		for idx, p in enumerate(PRODUCTS):
			c = selected.get(p, 0)
			label = f"{p} [{c}]"
			row.append(InlineKeyboardButton(text=label, callback_data=f"mic:set:{p}"))
			if (idx + 1) % 2 == 0:
				buttons.append(row); row = []
		if row: buttons.append(row)
		buttons.append([InlineKeyboardButton(text="Готово", callback_data="mic:done"), InlineKeyboardButton(text="Отмена", callback_data="mic:cancel")])
		return InlineKeyboardMarkup(inline_keyboard=buttons)

	@dp.message(F.text == "Массовая выдача")
	async def mass_issue_start(message: Message, state: FSMContext) -> None:
		uid = message.from_user.id
		if not db.is_allowed(uid):
			await message.answer("Доступ ограничен."); return
		if not db.work_is_open(uid):
			await message.answer("Начните рабочий день, чтобы пользоваться функциями", reply_markup=_kb_work_open()); return
		await state.set_state(MassIssueStates.entering)
		await state.update_data(mi={"zp": 0, "cross": {}, "awaiting": None})
		await message.answer("Введите количество выданных ЗП (число)", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="mi:cancel")]]))

	@dp.message(MassIssueStates.entering)
	async def mass_issue_set_count(message: Message, state: FSMContext) -> None:
		text = (message.text or "").strip()
		try:
			n = int(text)
			if n < 0 or n > 10000:
				raise ValueError()
		except Exception:
			await message.answer("Введите целое число от 0 до 10000")
			return
		data = await state.get_data()
		mi = data.get("mi", {})
		mi["zp"] = n
		await state.update_data(mi=mi)
		await message.answer(f"ЗП: {n}. Что дальше?", reply_markup=_kb_mass_issue_post_count())

	@dp.callback_query(F.data == "mi:cancel")
	async def mass_issue_cancel(call: CallbackQuery, state: FSMContext) -> None:
		await state.clear()
		await call.message.answer("Результат не сохранен", reply_markup=main_keyboard())
		await call.answer()

	@dp.callback_query(F.data == "mi:done")
	async def mass_issue_done(call: CallbackQuery, state: FSMContext) -> None:
		data = await state.get_data(); mi = data.get("mi", {})
		zp = int(mi.get("zp", 0) or 0)
		try:
			if zp > 0:
				db.save_attempts(call.from_user.id, {"ЗП": zp}, date.today())
				try:
					db.log(call.from_user.id, "mass_issue_saved", {"zp": zp})
				except Exception:
					pass
			await call.message.answer("Результат сохранен", reply_markup=main_keyboard())
		except Exception as e:
			db.log(call.from_user.id, "error", {"where": "mass_issue_done", "error": str(e)})
			await call.message.answer("Ошибка сохранения. Повторите позже.", reply_markup=main_keyboard())
		finally:
			await state.clear(); await call.answer()

	@dp.callback_query(F.data == "mi:cross")
	async def mass_issue_to_cross(call: CallbackQuery, state: FSMContext) -> None:
		data = await state.get_data(); mi = data.get("mi", {})
		cross = mi.get("cross") or {}
		await state.set_state(MassIssueStates.cross_selecting)
		await state.update_data(mi={"zp": int(mi.get("zp", 0) or 0), "cross": cross, "awaiting": None})
		await call.message.answer("Внесите кросс-продажи: выберите продукт, затем введите количество", reply_markup=_kb_mass_cross(cross))
		await call.answer()

	@dp.callback_query(MassIssueStates.cross_selecting, F.data.startswith("mic:set:"))
	async def mass_cross_pick(call: CallbackQuery, state: FSMContext) -> None:
		p = call.data.split(":",2)[2]
		data = await state.get_data(); mi = data.get("mi", {})
		mi["awaiting"] = p
		await state.update_data(mi=mi)
		await call.message.answer(f"Введите количество для {p}")
		await state.set_state(MassIssueStates.cross_wait_count)
		await call.answer()

	@dp.message(MassIssueStates.cross_wait_count)
	async def mass_cross_set_count(message: Message, state: FSMContext) -> None:
		text = (message.text or "").strip()
		try:
			n = int(text)
			if n < 0 or n > 10000:
				raise ValueError()
		except Exception:
			await message.answer("Введите целое число от 0 до 10000")
			return
		data = await state.get_data(); mi = data.get("mi", {})
		p = mi.get("awaiting")
		if not p:
			await message.answer("Не выбран продукт. Выберите продукт из списка.")
			await state.set_state(MassIssueStates.cross_selecting)
			return
		cross = mi.get("cross") or {}
		cross[p] = n
		mi["cross"] = cross; mi["awaiting"] = None
		await state.update_data(mi=mi)
		await message.answer(f"{p}: {n}", reply_markup=_kb_mass_cross(cross))
		await state.set_state(MassIssueStates.cross_selecting)

	@dp.callback_query(MassIssueStates.cross_selecting, F.data == "mic:cancel")
	async def mass_cross_cancel(call: CallbackQuery, state: FSMContext) -> None:
		await state.clear()
		await call.message.answer("Результат не сохранен", reply_markup=main_keyboard())
		await call.answer()

	@dp.callback_query(MassIssueStates.cross_selecting, F.data == "mic:done")
	async def mass_cross_done(call: CallbackQuery, state: FSMContext) -> None:
		data = await state.get_data(); mi = data.get("mi", {})
		zp = int(mi.get("zp", 0) or 0); cross = mi.get("cross") or {}
		try:
			# save ZP if provided
			if zp > 0:
				db.save_attempts(call.from_user.id, {"ЗП": zp}, date.today())
			# save cross attempts
			if cross:
				db.save_attempts(call.from_user.id, cross, date.today())
				try:
					db.log(call.from_user.id, "save_attempts", cross)
				except Exception:
					pass
			await call.message.answer("Результат сохранен", reply_markup=main_keyboard())
		except Exception as e:
			try:
				db.log(call.from_user.id, "error", {"where": "mass_cross_done", "error": str(e)})
			except Exception:
				pass
			await call.message.answer("Ошибка сохранения. Повторите позже.", reply_markup=main_keyboard())
		finally:
			await state.clear(); await call.answer()

	# Workday control
	@dp.message(F.text == "Начать работу")
	async def work_open_handler(message: Message) -> None:
		user_id = message.from_user.id
		if not db.is_allowed(user_id):
			await message.answer("Доступ ограничен.")
			return
		db.work_open(user_id)
		plan = db.compute_plan_breakdown(user_id, date.today())
		pen_target = int(plan.get('penetration_target_pct', 50))
		await message.answer(f"🎯 Цель на сегодня: не менее {pen_target}% проникновения кросс‑продаж во встречи. У тебя всё получится! 💪", reply_markup=main_keyboard())

	@dp.message(F.text == "Завершить работу")
	async def work_close_handler(message: Message) -> None:
		user_id = message.from_user.id
		if not db.is_allowed(user_id):
			await message.answer("Доступ ограничен.")
			return
		db.work_close(user_id)
		# Build end-of-day report
		today = date.today()
		stats = db.stats_day_week_month(user_id, today)
		plan = db.compute_plan_breakdown(user_id, today)
		pen_target = int(plan.get('penetration_target_pct', 50))
		start_week = today - timedelta(days=today.weekday())
		start_month = today.replace(day=1)
		m_day = db.meets_period_count(user_id, today, today)
		m_week = db.meets_period_count(user_id, start_week, today)
		m_month = db.meets_period_count(user_id, start_month, today)
		linked_day = db.attempts_linked_period_count(user_id, today, today)
		linked_week = db.attempts_linked_period_count(user_id, start_week, today)
		linked_month = db.attempts_linked_period_count(user_id, start_month, today)
		pen_day = (linked_day * 100 / m_day) if m_day > 0 else 0
		pen_week = (linked_week * 100 / m_week) if m_week > 0 else 0
		pen_month = (linked_month * 100 / m_month) if m_month > 0 else 0
		# previous penetrations for Δ (percentage-point difference)
		m_prev_day = db.meets_period_count(user_id, today - timedelta(days=1), today - timedelta(days=1))
		linked_prev_day = db.attempts_linked_period_count(user_id, today - timedelta(days=1), today - timedelta(days=1))
		pen_prev_day = (linked_prev_day * 100 / m_prev_day) if m_prev_day > 0 else 0
		start_prev_w = start_week - timedelta(days=7)
		end_prev_w = start_week - timedelta(days=1)
		m_prev_week = db.meets_period_count(user_id, start_prev_w, end_prev_w)
		linked_prev_week = db.attempts_linked_period_count(user_id, start_prev_w, end_prev_w)
		pen_prev_week = (linked_prev_week * 100 / m_prev_week) if m_prev_week > 0 else 0
		end_prev_m = start_month - timedelta(days=1)
		start_prev_m = end_prev_m.replace(day=1)
		m_prev_month = db.meets_period_count(user_id, start_prev_m, end_prev_m)
		linked_prev_month = db.attempts_linked_period_count(user_id, start_prev_m, end_prev_m)
		pen_prev_month = (linked_prev_month * 100 / m_prev_month) if m_prev_month > 0 else 0
		def _delta_pp(curr_pct: float, prev_pct: float) -> int:
			return int(round(curr_pct - prev_pct))
		today_total = int(stats['today']['total'])
		week_total = int(stats['week']['total'])
		month_total = int(stats['month']['total'])
		d_pen_day = _delta_pp(int(round(pen_day)), int(round(pen_prev_day)))
		d_pen_week = _delta_pp(int(round(pen_week)), int(round(pen_prev_week)))
		d_pen_month = _delta_pp(int(round(pen_month)), int(round(pen_prev_month)))
		# products line today
		items = [(p, c) for p, c in (stats['today']['by_product'] or {}).items() if c > 0]
		items.sort(key=lambda x: (-x[1], x[0]))
		breakdown = ", ".join([f"{c}{p}" for p, c in items]) if items else "—"
		lines = []
		lines.append(f"- Сегодня: {int(round(pen_day))}% ({today_total}шт.) факт / {pen_target}% план / {int(round((pen_day/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_day}%")
		lines.append(f"- Сегодня по продуктам: — {breakdown}")
		lines.append(f"- Неделя: {int(round(pen_week))}% ({week_total}шт.) факт / {pen_target}% план / {int(round((pen_week/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_week}%")
		lines.append(f"- Месяц: { _fmt1(pen_month)}% ({month_total}шт.) факт / {pen_target}% план / {int(round((pen_month/(pen_target if pen_target>0 else 1))*100))}% выполнение / Δ {d_pen_month}%")
		# Build AI prompt with embedded stats JSON so the model uses current results
		payload = {
			"period": {"label": "День", "start": today.isoformat(), "end": today.isoformat()},
			"current": {
				"day":   {"cross_fact": today_total,  "meet": m_day,   "penetration_pct": int(round(pen_day))},
				"week":  {"cross_fact": week_total,   "meet": m_week,  "penetration_pct": int(round(pen_week))},
				"month": {"cross_fact": month_total,  "meet": m_month, "penetration_pct": int(round(pen_month))},
			},
			"previous": {
				"day":   {"penetration_pct": int(round(pen_prev_day))},
				"week":  {"penetration_pct": int(round(pen_prev_week))},
				"month": {"penetration_pct": int(round(pen_prev_month))},
			},
			"targets": {"penetration_target_pct": pen_target}
		}
		policy = (
			"[EOD_POLICY]\n"
			"Задача: оцени результаты сотрудника по STATS_JSON и дай краткие рекомендации.\n"
			"Правила: используй ТОЛЬКО числа из STATS_JSON; не выдумывай. Сфокусируйся на повышении конверсии, не предлагай увеличивать количество встреч.\n"
			"Формат: 1) Выводы 2) Рекомендации/план на завтра.\n"
		)
		ai_prompt = policy + "\n[STATS_JSON]\n" + json.dumps(payload, ensure_ascii=False)
		month_rank = db.month_ranking(start_month, today)
		emp = db.get_or_register_employee(user_id)
		reply_ai = get_assistant_reply(db, user_id, (emp.agent_name if emp else "Агент"), stats, month_rank, ai_prompt)
		# Send a single combined message
		final_msg = "\n".join(lines) + "\n" + reply_ai
		await message.answer(final_msg, reply_markup=_kb_work_open())

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
		if not db.work_is_open(user_id):
			await message.answer("Начните рабочий день, чтобы пользоваться функциями", reply_markup=_kb_work_open())
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
			await call.answer()

	# Meet flow
	@dp.message(F.text == "Внести встречу")
	@dp.message(Command("meet"))
	@dp.message(lambda m: (m.text or "").strip().lower() == "внести встречу")
	async def meet_start(message: Message, state: FSMContext) -> None:
		user_id = message.from_user.id
		if not db.is_allowed(user_id):
			await message.answer("Доступ ограничен.")
			return
		if not db.work_is_open(user_id):
			await message.answer("Начните рабочий день, чтобы пользоваться функциями", reply_markup=_kb_work_open())
			return
		await state.set_state(MeetStates.selecting)
		await state.update_data(meet=MeetSession(product=None).__dict__)
		await message.answer("Выберите продукт доставки (один на сессию)", reply_markup=meet_keyboard(None))

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
		user_id = message.from_user.id
		if not db.is_allowed(user_id):
			await message.answer("Доступ ограничен.")
			return
		emp = db.get_or_register_employee(user_id)
		if not emp:
			await message.answer("Временная ошибка базы. Повторите позже.")
			return
		# Use employee timezone for personal 'today'
		import pytz as _pytz
		tz_name = db.get_employee_timezone(user_id)
		try:
			today = _pytz.timezone(tz_name).localize(datetime.now()).date()
		except Exception:
			today = date.today()
		stats = db.stats_day_week_month(user_id, today)
		plan = db.compute_plan_breakdown(user_id, today)
		month_rank = db.month_ranking(today.replace(day=1), today)
		pos = next((i+1 for i, r in enumerate(month_rank) if r["tg_id"] == user_id), None)
		# Compute group Top-2 by application timezone (common reference)
		try:
			app_tz = _pytz.timezone(get_settings().timezone)
			top2_date = app_tz.localize(datetime.now()).date()
		except Exception:
			top2_date = today
		top2, bottom2 = db.day_top_bottom(top2_date)
		top_str = ", ".join([r["agent_name"] for r in top2]) if top2 else "—"
		bottom_str = ", ".join([r["agent_name"] for r in bottom2]) if bottom2 else "—"
		day_total = int(stats['today']['total'])
		week_total = int(stats['week']['total'])
		month_total = int(stats['month']['total'])
		pen_target = int(plan.get('penetration_target_pct', 50))
		start_week = today - timedelta(days=today.weekday())
		start_month = today.replace(day=1)
		m_day = db.meets_period_count(user_id, today, today)
		m_week = db.meets_period_count(user_id, start_week, today)
		m_month = db.meets_period_count(user_id, start_month, today)
		linked_day = db.attempts_linked_period_count(user_id, today, today)
		linked_week = db.attempts_linked_period_count(user_id, start_week, today)
		linked_month = db.attempts_linked_period_count(user_id, start_month, today)
		def _fmt1(val: float | int) -> str:
			try:
				v = float(val)
				if abs(v - round(v)) < 1e-9:
					return str(int(round(v)))
				return f"{v:.1f}".replace('.', ',')
			except Exception:
				return str(val)
		pen_day = (linked_day * 100 / m_day) if m_day > 0 else 0
		pen_week = (linked_week * 100 / m_week) if m_week > 0 else 0
		pen_month = (linked_month * 100 / m_month) if m_month > 0 else 0
		lines: List[str] = []
		lines.append(f"🏆 Агент: {emp.agent_name} — место за месяц: {pos if pos else '—'}")
		lines.append("1. Сегодня:")
		lines.append(f"- встречи: {m_day} / проникновение {_fmt1(pen_day)}% (цель {pen_target}%)")
		lines.append(f"- кросс продажи: {day_total} факт")
		lines.append("2. Неделя:")
		lines.append(f"- встречи: {m_week} / проникновение {_fmt1(pen_week)}% (цель {pen_target}%)")
		lines.append(f"- кросс продажи: {week_total} факт")
		lines.append("3. Месяц:")
		lines.append(f"- встречи: {m_month} / проникновение {_fmt1(pen_month)}% (цель {pen_target}%)")
		lines.append(f"- кросс продажи: {month_total} факт")
		lines.append(f"🥇 Топ-2 сегодня: {top_str}")
		lines.append(f"🧱 Антилидеры: {bottom_str}")
		await message.answer("\n".join(lines), reply_markup=main_keyboard())

	# Notes and Assistant handlers below remain unchanged
	@dp.message(F.text == "Заметки")
	@dp.message(Command("notes"))
	@dp.message(lambda m: (m.text or "").strip().lower() == "заметки")
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
		if not db.work_is_open(message.from_user.id):
			await message.answer("Начните рабочий день, чтобы пользоваться функциями", reply_markup=_kb_work_open())
			return
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
			await message.answer("Временная ошибка. Попробуйте ещё раз.", reply_markup=main_keyboard())

	# FINAL CATCH-ALL: forward any unmatched text into assistant mode
	@dp.message()
	async def catch_all(message: Message, state: FSMContext) -> None:
		# If not allowed, short-circuit
		if not db.is_allowed(message.from_user.id):
			return
		if not db.work_is_open(message.from_user.id):
			await message.answer("Начните рабочий день, чтобы пользоваться функциями", reply_markup=_kb_work_open())
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
			await message.answer("Временная ошибка. Попробуйте ещё раз.", reply_markup=main_keyboard()) 