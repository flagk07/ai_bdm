### Взаимодействие систем (Mermaid диаграммы)

#### Webhook и хендлеры
```mermaid
flowchart TD
  User["Пользователь в Telegram"] --> TGAPI["Telegram API"]
  TGAPI -- webhook --> Webhook["FastAPI /webhook"]
  Webhook --> Router["aiogram Router"]
  Router --> Cmds["Команды/кнопки (handlers.py)"]
  Router --> Assistant["Помощник (assistant.py)"]
  Router --> Notes["Заметки (handlers.py)"]
  Cmds --> DB["Supabase (db.py)"]
  Assistant --> DB
  Notes --> DB
  DB --> Reply["Ответ в Telegram"]
  Reply --> TGAPI --> User
```

#### Внести результат (чек‑боксы)
```mermaid
flowchart TD
  Btn["Кнопка 'Внести результат'"] --> Checks["Чек‑боксы продуктов"]
  Checks --> Done["Готово"]
  Done --> Attempts["attempts: +1/продукт (сегодня)"]
  Attempts --> SB[(Supabase)]
  SB --> Summary["Сводка D/W/M"]
  Summary --> OutTG["Telegram"]
```

#### Статистика (/stats)
```mermaid
flowchart TD
  Stats["/stats"] --> DWM["db.stats_day_week_month"]
  DWM --> Plan["db.compute_plan_breakdown (план D/W/M, RR)"]
  DWM --> Rank["db.month_ranking (место)"]
  DWM --> Top["db.day_top_bottom (топ/анти)"]
  Plan --> Render["Рендер строк (факт/план + RR)"]
  Rank --> Render
  Top --> Render
  Render --> TG["Telegram"]
```

#### Помощник (ИИ)
```mermaid
flowchart TD
  Ask["/assistant → сообщение"] --> San["PII‑санитизация"]
  San --> Off{Оффтоп?}
  Off -- да --> Redirect["Редирект в рабочую тему"] --> TG
  Off -- нет --> Ctx["Контекст: стата, планы, заметки, 10 сообщений"]
  Ctx --> LLM["OpenAI gpt-4o-mini"]
  LLM --> Ans["Ответ по пунктам"]
  Ans --> Save["assistant_messages (off_topic=false)"]
  Ans --> TG["Telegram"]
```

#### Авто‑сводка (APScheduler)
```mermaid
flowchart TD
  Timer["APScheduler */5 мин"] --> Emps["Активные сотрудники"]
  Emps --> Calc["D/W/M, разбор по продуктам"]
  Calc --> Deltas["Δ к прошлым периодам (если зарегистрирован)"]
  Calc --> Plans["План D/W/M"]
  Calc --> RR["RR месяца"]
  Deltas --> Mode{AI_SUMMARY=on?}
  Plans --> Mode
  RR --> Mode
  Mode -- да --> AICom["assistant.py комментарий"]
  Mode -- нет --> Coach["Детерминированные коуч‑пункты"]
  AICom --> Text["Текст авто‑сводки"]
  Coach --> Text
  Text --> TGT["Telegram"]
```

#### Модель данных (Supabase)
```mermaid
flowchart TD
  Allowed["allowed_users"] --> Emp["employees (created_at)"]
  Emp --> Att["attempts"]
  Emp --> Notes["notes"]
  Emp --> AMsg["assistant_messages (off_topic)"]
  Emp --> Plans["sales_plans"]
  Att --> Logs["logs"]
  Notes --> Logs
  AMsg --> Logs
``` 