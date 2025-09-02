# AI BDM Telegram Bot (MVP)

## Быстрый старт
1. Создайте файл `.env` по образцу `.env.example` и заполните секреты.
2. Установите зависимости: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`.
3. Примените SQL-скрипт из `supabase/schema.sql` в Supabase SQL Editor.
4. Запустите бота: `python src/bot.py`.

## Переменные окружения
- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота.
- `OPENAI_API_KEY` — ключ OpenAI (GPT-5 совместимый API).
- `SUPABASE_URL` и `SUPABASE_API_KEY` — доступ к Supabase.
- `ALLOWED_TG_IDS` — список Telegram ID, которым разрешён доступ (опционально).
- `APP_TIMEZONE` — часовой пояс для расписания (по умолчанию `Europe/Moscow`).
- `ASSISTANT_MODEL` — модель LLM для ответов ассистента (по умолчанию `gpt5`).

## Команды бота
- `/start` — регистрация и присвоение `agentN`.
- `Внести результат` — учёт попыток кросс-продаж.
- `Статистика` — результаты за день/неделю/месяц и рейтинг.
- `Помощник` — ИИ-коуч на базе OpenAI.
- `Заметки` — сохранение и просмотр заметок.

## Безопасность и PD
- Секреты только в `.env`.
- Данные валидируются и очищаются от ПДн (телефоны, e-mail и т.п.).
- Все события и действия логируются в Supabase. 