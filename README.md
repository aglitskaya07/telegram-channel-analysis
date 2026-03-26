# Парсинг Telegram и анализ упоминаний брендов

## Скрипты (корень проекта)

| Файл | Назначение |
|------|------------|
| `scrape.py` | Выгрузка каналов через Telethon → `exports/` |
| `analyze_channels.py` | Поиск упоминаний, отчёт → `reports/report.md` |
| `content_enrich.py` | Транскрипты YouTube для LLM-разметки |
| `merge_sentiment_batches.py` | Склейка `artifacts/sentiment_batch_*.json` → `sentiment_overrides.json` |
| `llm_sentiment_label.py` | *Опционально:* разметка через OpenAI API из терминала |

## Папки

| Папка | Содержимое |
|-------|------------|
| `exports/` | JSON каналов (формат Desktop Export), результат `scrape.py` |
| `reports/` | Итоговый `report.md`, дампы `posts_dump.txt`, досье по каналам |
| `artifacts/` | `llm_sentiment_queue.jsonl`, при разметке по частям — `sentiment_batch_*.json` |
| `docs/` | Описание процесса (`WORKFLOW_REPORT.md`) |
| `legacy/` | Старые ручные экспорты, если ещё подключены в `MANUAL_CHANNEL_FILES` |
| `.cursor/rules/` | Правила для агента в Cursor |

## Данные и секреты (не коммитить)

- `sentiment_overrides.json` — разметка тональности  
- `tg_session.session`, `.tg_credentials.json` — сессия Telegram и API-ключи  

См. `.gitignore`.

## Зависимости

```bash
pip install -r requirements.txt
```

## Тот же пайплайн в новом проекте

Пошаговый чеклист (что копировать, что нет): **`docs/NEW_PROJECT_CHECKLIST.md`**.
