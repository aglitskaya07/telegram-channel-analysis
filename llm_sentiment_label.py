#!/usr/bin/env python3
"""
Опционально: разметка тональности через внешний OpenAI API из терминала.

Основной сценарий в Cursor — разметка агентом в чате по очереди
artifacts/llm_sentiment_queue.jsonl → sentiment_overrides.json (см. .cursor/rules).

Этот скрипт нужен только если явно хотите вызывать OpenAI без участия агента.

Ключ: OPENAI_API_KEY или .env в корне проекта.
  pip install openai
  python3 llm_sentiment_label.py

Модель: OPENAI_MODEL или gpt-4o-mini.
"""

import json
import os
import re
from pathlib import Path

try:
    from openai import OpenAI
except ImportError:
    raise SystemExit("Установите зависимость: pip install openai")

BASE_DIR = Path(__file__).resolve().parent
QUEUE_PATH = BASE_DIR / "artifacts" / "llm_sentiment_queue.jsonl"
OVERRIDES_PATH = BASE_DIR / "sentiment_overrides.json"

ALLOWED = {"позитивная", "скорее позитивная", "нейтральная", "скорее негативная", "негативная"}


def load_env_file():
    """Подхватывает .env без зависимости python-dotenv (не перезаписывает уже заданные в shell)."""
    env_path = BASE_DIR / ".env"
    if not env_path.is_file():
        return
    try:
        raw = env_path.read_text(encoding="utf-8")
    except OSError:
        return
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        if key:
            os.environ.setdefault(key, val)


def load_queue():
    if not QUEUE_PATH.exists():
        return []
    rows = []
    with QUEUE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def load_overrides():
    if not OVERRIDES_PATH.exists():
        return {}
    with OVERRIDES_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _parse_json_loose(text: str):
    text = (text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def classify(client, item, model: str):
    extra = ""
    if item.get("transcript_enriched") or "--- Транскрипт YouTube" in item.get("text", ""):
        extra = (
            "Если ниже есть блок «Транскрипт YouTube», оценивай тональность по содержанию ролика, "
            "а не только по короткой подписи к посту (подпись может быть нейтральным анонсом).\n"
        )
    user_prompt = (
        "Определи тональность автора к бренду в этом посте.\n"
        + extra
        + "Верни JSON-объект с полями sentiment и reason.\n"
        "sentiment только из списка: позитивная, скорее позитивная, нейтральная, скорее негативная, негативная.\n"
        "Оценивай именно отношение к бренду, не общий эмоциональный тон текста.\n\n"
        f"Бренд: {item['brand']}\n"
        f"Контекст: {item['context']}\n"
        f"Текст (подпись и при наличии — транскрипт видео):\n{item['text']}\n"
    )
    kwargs = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "Отвечай только одним JSON-объектом, без markdown и пояснений вне JSON.",
            },
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
    }
    try:
        resp = client.chat.completions.create(
            **kwargs,
            response_format={"type": "json_object"},
        )
    except Exception:
        resp = client.chat.completions.create(**kwargs)

    text = (resp.choices[0].message.content or "").strip()
    try:
        data = _parse_json_loose(text)
    except json.JSONDecodeError:
        return {"sentiment": "нейтральная", "reason": f"fallback parse error: {text[:180]}"}
    sentiment = data.get("sentiment", "нейтральная")
    if sentiment not in ALLOWED:
        sentiment = "нейтральная"
    reason = str(data.get("reason", "")).strip()[:300]
    return {"sentiment": sentiment, "reason": reason}


def run_labeling():
    """
    Размечает очередь через OpenAI Chat Completions.
    Возвращает (число новых оценок, None) при успехе или (0, сообщение об ошибке).
    """
    load_env_file()
    if not os.getenv("OPENAI_API_KEY"):
        return 0, "OPENAI_API_KEY не задан (export или файл .env в корне проекта)"

    queue = load_queue()
    if not queue:
        print("Очередь пуста: artifacts/llm_sentiment_queue.jsonl")
        return 0, None

    overrides = load_overrides()
    pending = [item for item in queue if item["post_key"] not in overrides]
    if not pending:
        print("Новых постов для разметки не было (все ключи уже в sentiment_overrides.json).")
        return 0, None

    client = OpenAI()
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    done = 0

    for item in pending:
        key = item["post_key"]
        try:
            overrides[key] = classify(client, item, model)
        except Exception as e:
            overrides[key] = {
                "sentiment": "нейтральная",
                "reason": f"ошибка API: {e!s}"[:300],
            }
        done += 1
        if done % 20 == 0:
            print(f"Размечено: {done}")

    with OVERRIDES_PATH.open("w", encoding="utf-8") as f:
        json.dump(overrides, f, ensure_ascii=False, indent=2)
    print(f"Готово. Добавлено {done} LLM-оценок в {OVERRIDES_PATH}")
    return done, None


def main():
    done, err = run_labeling()
    if err:
        raise SystemExit(err)


if __name__ == "__main__":
    main()
