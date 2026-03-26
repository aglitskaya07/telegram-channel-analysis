#!/usr/bin/env python3
"""
Склеивает JSON-фрагменты разметки тональности в один sentiment_overrides.json.

Когда агент Cursor размечает очередь по частям, сохраняйте каждую часть как:
  artifacts/sentiment_batch_01.json
  artifacts/sentiment_batch_02.json
  …

Каждый файл — объект вида {"channel|msg_id|brand": {"sentiment": "...", "reason": "..."}, ...}

Запуск без аргументов подхватывает artifacts/sentiment_batch_*.json по имени.
  python3 merge_sentiment_batches.py
  python3 merge_sentiment_batches.py artifacts/sentiment_batch_01.json artifacts/sentiment_batch_02.json
"""

import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent
OUT = BASE / "sentiment_overrides.json"


def main():
    if len(sys.argv) > 1:
        paths = [Path(p) for p in sys.argv[1:]]
    else:
        art = BASE / "artifacts"
        paths = sorted(art.glob("sentiment_batch_*.json"))
        if not paths:
            print(
                "Нет файлов artifacts/sentiment_batch_*.json — "
                "передайте пути к JSON-фрагментам аргументами.",
                file=sys.stderr,
            )
            sys.exit(1)

    merged = {}
    for p in paths:
        if not p.is_file():
            print(f"Пропуск (нет файла): {p}", file=sys.stderr)
            continue
        with p.open("r", encoding="utf-8") as f:
            chunk = json.load(f)
        if not isinstance(chunk, dict):
            print(f"Ожидался JSON-объект в {p}", file=sys.stderr)
            sys.exit(1)
        overlap = set(merged) & set(chunk)
        if overlap:
            print(f"Предупреждение: в {p} пересекаются ключи с предыдущими: {len(overlap)} шт.")
        merged.update(chunk)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    print(f"Записано {len(merged)} ключей в {OUT}")


if __name__ == "__main__":
    main()
