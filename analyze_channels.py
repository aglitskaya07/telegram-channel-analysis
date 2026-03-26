#!/usr/bin/env python3
"""
Анализ ТГ-каналов: упоминания Яндекса и конкурентов (VK, Сбер, Ozon) по авторам.
Генерирует reports/report.md и отдельные срезы reports/by_author/*.md
(по каждому автору: Яндекс и конкуренты — темы, тональность, долгосрочная линия).

Режимы:
  python3 analyze_channels.py                  — генерирует reports/report.md и отчёты по авторам
  python3 analyze_channels.py --dump-posts     — выгружает посты в reports/posts_dump.txt
  python3 analyze_channels.py --dump-yandex    — только Яндекс, по файлу на автора в reports/yandex_by_author/

Тональность к брендам: см. .cursor/rules — разметка агентом Cursor по artifacts/llm_sentiment_queue.jsonl → sentiment_overrides.json
"""

import argparse
import json
import re
import os
from collections import defaultdict, Counter

from content_enrich import enrich_text_for_llm

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORTS_DIR = os.path.join(BASE_DIR, "exports")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
ARTIFACTS_DIR = os.path.join(BASE_DIR, "artifacts")

MANUAL_CHANNEL_FILES = [
    ("legacy/result.json", "Android Broadcast", "Кирилл Розов"),
    ("ChatExport_2026-03-25/result.json", "Грокаем C++", None),
    ("ChatExport_2026-03-25 (1)/result.json", "Аня Подображных [Будни продакта]", "Аня Подображных"),
    ("ChatExport_2026-03-25 (2)/result.json", "Kotlin Developer", None),
    ("ChatExport_2026-03-25 (3)/result.json", "Mobile Developer", "Алексей Гладков / Pavel Kachan"),
    ("ChatExport_2026-03-25 (4)/result.json", "Android Good Reads", None),
]


def discover_exports(exports_dir=None):
    """Auto-discover JSON files produced by scrape.py in the exports/ folder.

    Returns list of (filepath, channel_name, author_label) tuples,
    compatible with MANUAL_CHANNEL_FILES format.
    """
    exports_dir = exports_dir or EXPORTS_DIR
    if not os.path.isdir(exports_dir):
        return []

    results = []
    for fname in sorted(os.listdir(exports_dir)):
        if not fname.endswith(".json"):
            continue
        fpath = os.path.join(exports_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue

        if "messages" not in data:
            continue

        channel_name = data.get("name", fname.replace(".json", ""))

        authors = [m.get("author") for m in data["messages"] if m.get("author")]
        if authors:
            top_authors = Counter(authors).most_common()
            if len(top_authors) == 1:
                author_label = top_authors[0][0]
            elif len(top_authors) <= 3:
                author_label = " / ".join(a for a, _ in top_authors)
            else:
                author_label = None
        else:
            author_label = None

        results.append((fpath, channel_name, author_label))
    return results


def get_channel_files():
    """Return combined list: exports/ take priority over manual Desktop Export files."""
    manual = [(os.path.join(BASE_DIR, rel), name, author)
              for rel, name, author in MANUAL_CHANNEL_FILES
              if os.path.exists(os.path.join(BASE_DIR, rel))]

    auto = discover_exports()

    auto_names = {name for _, name, _ in auto}
    manual_only = [(fp, name, author) for fp, name, author in manual
                   if name not in auto_names]

    if auto:
        print(f"Из exports/: {len(auto)} канал(ов) — "
              + ", ".join(name for _, name, _ in auto))
    if manual_only:
        print(f"Из ручных экспортов: {len(manual_only)} канал(ов) — "
              + ", ".join(name for _, name, _ in manual_only))

    return auto + manual_only

YANDEX_PATTERNS = {
    # Match Russian/English forms, including simple inflections: Яндекс, Яндекса, Yandex's, etc.
    "Яндекс (общее)": r'\bяндекс[а-яa-z-]*\b(?!\s*cup)',
    "Yandex (общее)": r"\byandex[a-z'-]*\b(?!\s*cup)",
    "YandexCup": r'yandex\s*cup',
    "AppMetrica": r'appmetrica[a-z]*',
    "Яндекс 360": r'яндекс[а-яa-z-]*\s*360',
    "Яндекс Диск": r'яндекс[а-яa-z-]*[\.\s]*диск[а-яa-z-]*',
    "Яндекс Музыка": r'яндекс[а-яa-z-]*[\.\s]*музык[а-яa-z-]*',
    "Яндекс Го": r'яндекс[а-яa-z-]*\s*го\b',
    "Яндекс Маркет": r'яндекс[а-яa-z-]*\s*маркет[а-яa-z-]*(?!инг|плейс)',
    "Яндекс Вертикали": r'яндекс[а-яa-z-]*\s*вертикал[а-яa-z-]*',
    "Авто.ру": r'авто\.ру',
    "Яндекс Недвижимость": r'яндекс[а-яa-z-]*\s*недвижимост[а-яa-z-]*',
    "Я.Субботник": r'я[\.\s]*субботник',
    "Яндекс ПРО": r'яндекс[а-яa-z-]*\s*про\b',
    "Яндекс Браузер": r'яндекс[а-яa-z-]*[\.\s]*браузер[а-яa-z-]*',
}

COMPETITOR_PATTERNS = {
    "VK": {
        "VK (платформа)": r'(?<![a-zA-Z/\.])vk[a-z-]*\b(?!\s*video|video)',
        "ВКонтакте": r'вконтакт[а-яa-z-]*',
        "VK Video": r'vk\s*video|vkvideo',
    },
    "Сбер": {
        "Сбер": r'\bсбер[а-яa-z-]*(?!маркет|мегамаркет)\b',
        "Sber": r'\bsber[a-z-]*\b',
        "GigaChat": r'gigachat[a-z-]*|гигачат[а-яa-z-]*',
    },
    "Ozon": {
        "Ozon": r'\bozon[a-z-]*\b',
        "Озон": r'\bозон[а-яa-z-]*\b',
    },
}

POSITIVE_SIGNALS = [
    r'круто', r'отлично', r'удобн', r'рекоменду', r'лучш[иеая]',
    r'классн', r'супер\b', r'мощн', r'полезн', r'нравится',
    r'прекрасн', r'потрясающ', r'офигел', r'огонь',
    r'хорош[оиеая]', r'впечатл',
]

NEGATIVE_SIGNALS = [
    r'проблем', r'\bбаг[аиов]?\b', r'ужасн', r'плох', r'кошмар',
    r'бесит', r'раздража', r'ненавиж', r'хейт', r'отстой',
    r'не работает', r'разочаров', r'говно',
]

AD_SIGNALS = [
    r'utm_', r'реклам', r'/jobs/', r'вакансии', r'vacancy',
    r'промокод', r'скидк', r'партнер', r'#ad\b', r'#реклама',
]


def flatten_text(text_field):
    if isinstance(text_field, str):
        return text_field
    if isinstance(text_field, list):
        parts = []
        for item in text_field:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                parts.append(item.get("text", ""))
        return "".join(parts)
    return ""


def extract_urls(text_field):
    urls = []
    if isinstance(text_field, list):
        for item in text_field:
            if isinstance(item, dict):
                href = item.get("href", "")
                if href:
                    urls.append(href)
                txt = item.get("text", "")
                if txt.startswith("http"):
                    urls.append(txt)
            elif isinstance(item, str):
                found = re.findall(r'https?://\S+', item)
                urls.extend(found)
    elif isinstance(text_field, str):
        found = re.findall(r'https?://\S+', text_field)
        urls.extend(found)
    return urls


def classify_context(text, urls):
    text_lower = text.lower()
    url_str = " ".join(urls).lower()

    for pat in AD_SIGNALS:
        if re.search(pat, text_lower) or re.search(pat, url_str):
            return "рекламный/партнерский"

    event_words = [r'митап', r'meetup', r'конференц', r'хакатон', r'hackathon',
                   r'субботник', r'регистрац', r'мероприят']
    for pat in event_words:
        if re.search(pat, text_lower):
            return "анонс мероприятия"

    if any(d in url_str for d in ['habr.com', 'youtube.com', 'youtu.be', 'medium.com']):
        return "контентный (статья/видео)"

    opinion_words = [r'считаю', r'думаю', r'по-моему', r'имхо', r'лично']
    for pat in opinion_words:
        if re.search(pat, text_lower):
            return "личное мнение/опыт"

    return "информационный"


def sentiment_near_brand(text_lower, brand_match_pos, window=300):
    """Score sentiment only in a window around the brand mention."""
    start = max(0, brand_match_pos - window)
    end = min(len(text_lower), brand_match_pos + window)
    snippet = text_lower[start:end]
    pos = sum(1 for p in POSITIVE_SIGNALS if re.search(p, snippet))
    neg = sum(1 for p in NEGATIVE_SIGNALS if re.search(p, snippet))
    return pos, neg


def score_sentiment_for_brand(text, patterns_dict):
    """Find brand mentions and score sentiment in context around each."""
    text_lower = text.lower()
    total_pos, total_neg = 0, 0
    matches_found = 0
    for label, pat in patterns_dict.items():
        for m in re.finditer(pat, text_lower):
            p, n = sentiment_near_brand(text_lower, m.start())
            total_pos += p
            total_neg += n
            matches_found += 1
    if matches_found > 0:
        total_pos = total_pos // matches_found
        total_neg = total_neg // matches_found
    return total_pos, total_neg


def sentiment_label(pos, neg):
    if pos > neg + 1:
        return "позитивная"
    elif neg > pos + 1:
        return "негативная"
    elif pos > 0 and neg == 0:
        return "скорее позитивная"
    elif neg > 0 and pos == 0:
        return "скорее негативная"
    else:
        return "нейтральная"


def get_month(date_str):
    return date_str[:7]


def find_mentions(text_lower, patterns_dict):
    hits = []
    for label, pat in patterns_dict.items():
        for m in re.finditer(pat, text_lower):
            hits.append((label, m.group(), m.start()))
    return hits


def load_channel(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def is_vk_only_hosting(text, urls):
    """Check if VK mention is only about VK Video as a hosting platform."""
    text_lower = text.lower()
    has_youtube = bool(re.search(r'youtube|youtu\.be', text_lower))
    vk_video_only = bool(re.search(r'vk\s*video|vkvideo|vk видео', text_lower))
    has_vk_editorial = bool(re.search(r'вконтакте|(?<![/\.a-z])vk(?!\s*video|video|\.|/)', text_lower))
    if has_youtube and vk_video_only and not has_vk_editorial:
        return True
    return False


def load_overrides():
    path = os.path.join(BASE_DIR, "sentiment_overrides.json")
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_overrides(overrides):
    path = os.path.join(BASE_DIR, "sentiment_overrides.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(overrides, f, ensure_ascii=False, indent=2)


def dump_posts(all_authors):
    lines = []
    idx = 0
    for author_key, info in sorted(all_authors.items()):
        for p in info["yandex_posts"]:
            idx += 1
            lines.append(f"={'='*80}")
            lines.append(f"POST #{idx}")
            lines.append(f"  key:      {p['post_key']}")
            lines.append(f"  author:   {author_key}")
            lines.append(f"  date:     {p['date']}")
            lines.append(f"  brand:    Яндекс")
            lines.append(f"  products: {', '.join(p['products'])}")
            lines.append(f"  context:  {p['context']}")
            lines.append(f"  current_sentiment: {p['sentiment']}")
            lines.append(f"-" * 40)
            lines.append(p["full_text"][:1500])
            lines.append("")

        for comp_name in ["VK", "Сбер", "Ozon"]:
            for p in info["competitor_posts"].get(comp_name, []):
                if p.get("vk_hosting"):
                    continue
                idx += 1
                lines.append(f"={'='*80}")
                lines.append(f"POST #{idx}")
                lines.append(f"  key:      {p['post_key']}")
                lines.append(f"  author:   {author_key}")
                lines.append(f"  date:     {p['date']}")
                lines.append(f"  brand:    {comp_name}")
                lines.append(f"  products: {', '.join(p['sub_products'])}")
                lines.append(f"  context:  {p['context']}")
                lines.append(f"  current_sentiment: {p['sentiment']}")
                lines.append(f"-" * 40)
                lines.append(p["full_text"][:1500])
                lines.append("")

    os.makedirs(REPORTS_DIR, exist_ok=True)
    out_path = os.path.join(REPORTS_DIR, "posts_dump.txt")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"Выгружено {idx} постов в {out_path}")
    return idx


def safe_report_filename(author_key: str) -> str:
    s = re.sub(r'[<>:"/\\|?*]', "_", author_key)
    s = re.sub(r"\s+", "_", s.strip())
    return s[:180] if len(s) > 180 else s


def dump_yandex_by_author(all_authors):
    """Отдельный файл на каждого автора/канал только с постами про Яндекс (полный текст)."""
    out_dir = os.path.join(REPORTS_DIR, "yandex_by_author")
    os.makedirs(out_dir, exist_ok=True)
    n_files = 0
    n_posts = 0
    for author_key, info in sorted(all_authors.items()):
        posts = info["yandex_posts"]
        if not posts:
            continue
        fn = safe_report_filename(author_key) + ".txt"
        path = os.path.join(out_dir, fn)
        lines = [
            f"# Посты с упоминанием Яндекса",
            f"# Автор / ключ: {author_key}",
            f"# Канал: {info['channel']}",
            f"# Всего постов в выборке: {len(posts)}",
            "",
        ]
        for i, p in enumerate(posts, 1):
            n_posts += 1
            lines.append("=" * 72)
            lines.append(f"### {i}. {p['date'][:19]} | {p['context']} | {p['sentiment']}")
            lines.append(f"Продукты/темы: {', '.join(p['products'])}")
            if p.get("sentiment_reason"):
                lines.append(f"Примечание к тональности: {p['sentiment_reason']}")
            lines.append("-" * 40)
            lines.append(p.get("full_text", "").strip())
            lines.append("")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        n_files += 1
        print(f"  {path}")
    print(f"Выгрузка про Яндекс: {n_files} файл(ов), {n_posts} пост(ов) в {out_dir}")
    return n_files


def save_llm_queue(items):
    if not items:
        return
    os.makedirs(ARTIFACTS_DIR, exist_ok=True)
    out_path = os.path.join(ARTIFACTS_DIR, "llm_sentiment_queue.jsonl")
    with open(out_path, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"Подготовлено {len(items)} постов для LLM-разметки: {out_path}")


def analyze(allow_keyword_fallback=False):
    overrides = load_overrides()
    if overrides:
        print(f"Загружено {len(overrides)} оценок тональности из sentiment_overrides.json")
    mode = "LLM-only" if not allow_keyword_fallback else "LLM + keyword fallback"
    print(f"Режим тональности: {mode}")
    all_authors = {}
    llm_queue = []
    overrides_dirty = False

    channel_files = get_channel_files()
    for filepath, channel_name, author_label in channel_files:
        data = load_channel(filepath)
        messages = [m for m in data["messages"] if m.get("type") == "message"]

        author_key = f"{author_label} ({channel_name})" if author_label else channel_name

        if author_key not in all_authors:
            all_authors[author_key] = {
                "channel": channel_name,
                "author": author_label or channel_name,
                "total_posts": 0,
                "yandex_posts": [],
                "competitor_posts": defaultdict(list),
                "months": set(),
            }

        info = all_authors[author_key]

        for msg in messages:
            info["total_posts"] += 1
            text = flatten_text(msg.get("text", ""))
            urls = extract_urls(msg.get("text", ""))
            text_lower = text.lower()
            date_str = msg.get("date", "")
            month = get_month(date_str)
            info["months"].add(month)

            yandex_hits = find_mentions(text_lower, YANDEX_PATTERNS)

            yandex_url_only = False
            if not yandex_hits:
                url_text = " ".join(urls).lower()
                if re.search(r'yandex|яндекс', url_text):
                    yandex_url_only = True
                    yandex_hits = [("Yandex (URL)", "yandex", 0)]

            if yandex_hits:
                ctx = classify_context(text, urls)
                products = list(set(label for label, _, _ in yandex_hits))

                if yandex_url_only and ctx == "информационный":
                    continue

                post_key = f"{channel_name}|{msg.get('id', '')}|yandex"
                enriched = enrich_text_for_llm(text, urls)
                if enriched["transcript_enriched"] and post_key in overrides:
                    del overrides[post_key]
                    overrides_dirty = True

                override = overrides.get(post_key)
                if override:
                    sent = override["sentiment"]
                    sent_reason = override.get("reason", "")
                else:
                    if allow_keyword_fallback:
                        pos, neg = score_sentiment_for_brand(text, YANDEX_PATTERNS)
                        sent = sentiment_label(pos, neg)
                        sent_reason = "keyword fallback (нет LLM-оценки)"
                    else:
                        sent = "не размечено (нужна LLM-оценка)"
                        sent_reason = ""
                    item = {
                        "post_key": post_key,
                        "channel": channel_name,
                        "msg_id": msg.get("id", ""),
                        "date": date_str,
                        "brand": "Яндекс",
                        "products": products,
                        "context": ctx,
                        "text": enriched["text"],
                        "caption_excerpt": text[:800],
                        "transcript_enriched": enriched["transcript_enriched"],
                        "youtube_video_ids": enriched.get("youtube_video_ids", []),
                    }
                    if enriched.get("transcript_note"):
                        item["transcript_note"] = enriched["transcript_note"]
                    llm_queue.append(item)

                info["yandex_posts"].append({
                    "date": date_str,
                    "month": month,
                    "products": products,
                    "context": ctx,
                    "sentiment": sent,
                    "sentiment_reason": sent_reason,
                    "text_preview": text[:400].replace("\n", " ").strip(),
                    "urls": urls,
                    "url_only": yandex_url_only,
                    "post_key": post_key,
                    "msg_id": msg.get("id", ""),
                    "full_text": text,
                })

            for comp_name, comp_patterns in COMPETITOR_PATTERNS.items():
                comp_hits = find_mentions(text_lower, comp_patterns)
                if not comp_hits:
                    continue

                if comp_name == "VK":
                    vk_hosting = is_vk_only_hosting(text, urls)
                else:
                    vk_hosting = False

                ctx = classify_context(text, urls)
                sub_products = list(set(label for label, _, _ in comp_hits))

                post_key = f"{channel_name}|{msg.get('id', '')}|{comp_name}"
                enriched = enrich_text_for_llm(text, urls)
                if enriched["transcript_enriched"] and post_key in overrides:
                    del overrides[post_key]
                    overrides_dirty = True

                override = overrides.get(post_key)
                if override:
                    sent = override["sentiment"]
                    sent_reason = override.get("reason", "")
                else:
                    if allow_keyword_fallback:
                        pos, neg = score_sentiment_for_brand(text, comp_patterns)
                        sent = sentiment_label(pos, neg)
                        sent_reason = "keyword fallback (нет LLM-оценки)"
                    else:
                        sent = "не размечено (нужна LLM-оценка)"
                        sent_reason = ""
                    item = {
                        "post_key": post_key,
                        "channel": channel_name,
                        "msg_id": msg.get("id", ""),
                        "date": date_str,
                        "brand": comp_name,
                        "products": sub_products,
                        "context": ctx,
                        "text": enriched["text"],
                        "caption_excerpt": text[:800],
                        "transcript_enriched": enriched["transcript_enriched"],
                        "youtube_video_ids": enriched.get("youtube_video_ids", []),
                    }
                    if enriched.get("transcript_note"):
                        item["transcript_note"] = enriched["transcript_note"]
                    llm_queue.append(item)

                info["competitor_posts"][comp_name].append({
                    "date": date_str,
                    "month": month,
                    "sub_products": sub_products,
                    "context": ctx,
                    "sentiment": sent,
                    "sentiment_reason": sent_reason,
                    "text_preview": text[:400].replace("\n", " ").strip(),
                    "vk_hosting": vk_hosting,
                    "post_key": post_key,
                    "msg_id": msg.get("id", ""),
                    "full_text": text,
                })

    save_llm_queue(llm_queue)
    if overrides_dirty:
        save_overrides(overrides)
        print("Обновлён sentiment_overrides.json: сняты оценки для постов с транскриптом YouTube (нужна повторная LLM-разметка).")
    return all_authors


def trajectory_label(posts, brand=""):
    if not posts:
        return "не упоминает"

    hosting_count = sum(1 for p in posts if p.get("vk_hosting"))
    if hosting_count == len(posts):
        return "использует VK Video как хостинг (не редакционный)"

    n = len(posts)
    if n <= 2:
        sentiments = [p["sentiment"] for p in posts]
        if all(s == "нейтральная" for s in sentiments):
            return "нейтральный (мало данных)"
        elif any("позитивная" in s for s in sentiments) and not any("негативная" in s for s in sentiments):
            return "скорее позитивный (мало данных)"
        elif any("негативная" in s for s in sentiments) and not any("позитивная" in s for s in sentiments):
            return "скорее негативный (мало данных)"
        else:
            return "эпизодический (мало данных)"

    non_hosting = [p for p in posts if not p.get("vk_hosting")]
    if non_hosting:
        posts_for_scoring = non_hosting
    else:
        posts_for_scoring = posts

    n_eff = len(posts_for_scoring)
    pos_count = sum(1 for p in posts_for_scoring if "позитивная" in p["sentiment"])
    neg_count = sum(1 for p in posts_for_scoring if "негативная" in p["sentiment"])
    neutral_count = sum(1 for p in posts_for_scoring if p["sentiment"] == "нейтральная")
    ad_count = sum(1 for p in posts_for_scoring if "рекламн" in p["context"])

    if neg_count > pos_count * 2 and neg_count >= 3:
        return "хейтер"
    elif neg_count > pos_count and neg_count >= 2:
        return "скорее негативный"
    elif pos_count > neg_count * 2 and pos_count >= 3:
        return "лоялист"
    elif pos_count > neg_count and pos_count >= 2:
        return "скорее позитивный"
    elif ad_count >= n_eff * 0.5:
        return "рекламный партнер"
    elif neutral_count >= n_eff * 0.6:
        return "нейтральный"
    else:
        return "нейтральный"


def _products_from_yandex_post(p):
    return p.get("products") or []


def _products_from_competitor_post(p):
    return p.get("sub_products") or []


def narrative_brand_slice(posts, brand_phrase, trajectory, get_products):
    """Краткий связный текст: темы/продукты, контекст, отношение, долгосрочная линия."""
    if not posts:
        return (
            f"За период **{brand_phrase}** в постах не встречается "
            f"(или только нерелевантные URL без текста)."
        )

    n = len(posts)
    prods = Counter()
    for p in posts:
        for x in get_products(p):
            prods[x] += 1
    top_labels = [f"{name} ({cnt})" for name, cnt in prods.most_common(6)]
    prod_line = ", ".join(top_labels) if top_labels else "—"

    ctx_c = Counter(p["context"] for p in posts)
    ctx_line = ", ".join(f"{c} ({k})" for c, k in ctx_c.most_common(5))

    sent_c = Counter(p["sentiment"] for p in posts)
    sent_line = ", ".join(f"{s}: {k}" for s, k in sent_c.most_common(8))

    pos_n = sum(1 for p in posts if "позитив" in p["sentiment"])
    neg_n = sum(1 for p in posts if "негатив" in p["sentiment"])
    unlabeled = sum(
        1 for p in posts if "не размечено" in p.get("sentiment", "")
    )

    parts = [
        f"**О чём писал** (темы/продукты, {n} пост(ов)): {prod_line}. ",
        f"**Контексты:** {ctx_line}. ",
        f"**Отношение к бренду** (по разметке постов): {sent_line}. ",
    ]
    if pos_n or neg_n:
        parts.append(
            f"Суммарно явнее **позитив** в {pos_n} пост(ах), **негатив** в {neg_n}. "
        )
    if unlabeled:
        parts.append(
            f"Без оценки тональности осталось {unlabeled} пост(ов) — разметь агентом Cursor "
            f"по `artifacts/llm_sentiment_queue.jsonl` → `sentiment_overrides.json` (см. .cursor/rules). "
        )
    if unlabeled == n and n > 0:
        parts.append(
            "**Долгосрочная линия** формально **нейтральная** (нет ни одной оценки тональности); "
            "после LLM-разметки метка «хейтер/лоялист» станет содержательной. "
        )
    else:
        parts.append(
            f"**Долгосрочная линия** (хейтер / лоялист и др.): **{trajectory}**."
        )
    return "".join(parts)


def generate_author_slice_md(author_key, info, period_str):
    """Один файл: отдельный срез по Яндексу и по каждому конкуренту."""
    lines = [
        f"# Срез: {author_key}",
        "",
        f"- **Канал:** {info['channel']}",
    ]
    if info["author"] != info["channel"]:
        lines.append(f"- **Автор(ы):** {info['author']}")
    lines.extend([
        f"- **Всего постов в выборке:** {info['total_posts']}",
        f"- **Период данных:** {period_str}",
        "",
        "---",
        "",
        "## Яндекс",
        "",
    ])

    yp = info["yandex_posts"]
    y_traj = trajectory_label(yp, "Яндекс")
    lines.append(narrative_brand_slice(yp, "Яндекса", y_traj, _products_from_yandex_post))
    lines.append("")

    if yp:
        lines.append("### Все публикации")
        lines.append("")
        for i, p in enumerate(yp, 1):
            d = (p.get("date") or "")[:10]
            prods = ", ".join(p.get("products") or [])
            reason = f" — {p['sentiment_reason']}" if p.get("sentiment_reason") else ""
            lines.append(
                f"**{i}.** [{d}] **{p['sentiment']}**{reason} · {p['context']} · {prods}"
            )
            full = (p.get("full_text") or "").strip()
            if full:
                for line in full.splitlines():
                    lines.append(f"> {line}")
            lines.append("")

    lines.extend([
        "---",
        "",
        "## Конкуренты (VK, Сбер, Ozon)",
        "",
    ])

    for comp_name in ["VK", "Сбер", "Ozon"]:
        cp = info["competitor_posts"].get(comp_name, [])
        traj = trajectory_label(cp, comp_name)
        lines.append(f"### {comp_name}")
        lines.append("")
        lines.append(
            narrative_brand_slice(
                cp, comp_name, traj, _products_from_competitor_post
            )
        )
        lines.append("")
        if not cp:
            continue
        hosting_note = ""
        if comp_name == "VK":
            hv = sum(1 for p in cp if p.get("vk_hosting"))
            if hv:
                hosting_note = f"\n\n*{hv} пост(ов) — кросс-постинг на VK Video (хостинг), показаны только содержательные.*"

        non_hosting = [p for p in cp if not p.get("vk_hosting")]
        show_posts = non_hosting if non_hosting else cp

        lines.append(f"**Все публикации** ({len(show_posts)} содержательных из {len(cp)}){hosting_note}")
        lines.append("")
        for i, p in enumerate(show_posts, 1):
            d = (p.get("date") or "")[:10]
            sub = ", ".join(p.get("sub_products") or [])
            reason = f" — {p['sentiment_reason']}" if p.get("sentiment_reason") else ""
            hosting_tag = " [VK Video хостинг]" if p.get("vk_hosting") else ""
            lines.append(
                f"**{i}.** [{d}] **{p['sentiment']}**{reason} · {p['context']}{hosting_tag} · {sub}"
            )
            full = (p.get("full_text") or "").strip()
            if full:
                for line in full.splitlines():
                    lines.append(f"> {line}")
            lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(
        "*Полная сводка по всем авторам: [`report.md`](../report.md)*"
    )
    return "\n".join(lines)


def write_by_author_slices(all_authors, period_str):
    """Отдельный markdown на каждого автора: `reports/by_author/<канал>.md`."""
    out_dir = os.path.join(REPORTS_DIR, "by_author")
    os.makedirs(out_dir, exist_ok=True)
    for old in os.listdir(out_dir):
        if old.endswith(".md"):
            os.remove(os.path.join(out_dir, old))
    paths = []
    for author_key, info in sorted(all_authors.items()):
        body = generate_author_slice_md(author_key, info, period_str)
        fn = safe_report_filename(info["channel"]) + ".md"
        path = os.path.join(out_dir, fn)
        with open(path, "w", encoding="utf-8") as f:
            f.write(body)
        paths.append(path)
    print(f"Срезы по авторам ({len(paths)} файлов): {out_dir}")
    return paths


def format_period_line(all_authors):
    all_months = set()
    for info in all_authors.values():
        all_months.update(info["months"])
    sorted_months = sorted(all_months)
    if not sorted_months:
        return "—"
    month_names = {
        "01": "январь", "02": "февраль", "03": "март", "04": "апрель",
        "05": "май", "06": "июнь", "07": "июль", "08": "август",
        "09": "сентябрь", "10": "октябрь", "11": "ноябрь", "12": "декабрь",
    }
    first_y, first_m = sorted_months[0].split("-")
    last_y, last_m = sorted_months[-1].split("-")
    return (
        f"{month_names.get(first_m, first_m)} {first_y} — "
        f"{month_names.get(last_m, last_m)} {last_y}"
    )


def generate_report(all_authors):
    period = format_period_line(all_authors)

    lines = []
    lines.append("# Анализ авторов ТГ-каналов: Яндекс и конкуренты\n")
    lines.append(f"**Период:** {period}  ")
    lines.append(f"**Каналов:** {len(all_authors)}  ")
    lines.append("**Конкуренты в фокусе:** VK/ВКонтакте, Сбер/GigaChat, Ozon\n")
    lines.append("---\n")

    sorted_authors = sorted(all_authors.items(),
                            key=lambda x: len(x[1]["yandex_posts"]), reverse=True)

    for author_key, info in sorted_authors:
        months_sorted = sorted(info["months"])
        lines.append(f"## {author_key}\n")
        lines.append(f"- **Канал:** {info['channel']}")
        if info["author"] != info["channel"]:
            lines.append(f"- **Автор(ы):** {info['author']}")
        lines.append(f"- **Всего постов:** {info['total_posts']}")
        lines.append(f"- **Период активности:** {months_sorted[0]} — {months_sorted[-1]}")
        lines.append("")

        # --- ЯНДЕКС ---
        yp = info["yandex_posts"]
        lines.append(f"### Яндекс ({len(yp)} пост(ов) с упоминаниями)\n")

        if not yp:
            lines.append("Упоминания Яндекса и его продуктов в постах за период **не обнаружены**.\n")
        else:
            all_products = defaultdict(int)
            contexts = defaultdict(int)
            sentiments = defaultdict(int)
            by_month = defaultdict(list)
            for p in yp:
                for prod in p["products"]:
                    all_products[prod] += 1
                contexts[p["context"]] += 1
                sentiments[p["sentiment"]] += 1
                by_month[p["month"]].append(p)

            lines.append("**Упоминаемые продукты/темы:**\n")
            for prod, cnt in sorted(all_products.items(), key=lambda x: -x[1]):
                lines.append(f"- {prod}: {cnt}")
            lines.append("")

            lines.append("**Контекст упоминаний:**\n")
            for ctx, cnt in sorted(contexts.items(), key=lambda x: -x[1]):
                lines.append(f"- {ctx}: {cnt}")
            lines.append("")

            lines.append("**Тональность:**\n")
            for sent, cnt in sorted(sentiments.items(), key=lambda x: -x[1]):
                lines.append(f"- {sent}: {cnt}")
            lines.append("")

            lines.append("**Динамика по месяцам:**\n")
            for m in sorted(by_month.keys()):
                posts_m = by_month[m]
                sents = [p["sentiment"] for p in posts_m]
                lines.append(f"- {m}: {len(posts_m)} пост(ов) — {', '.join(sents)}")
            lines.append("")

            traj = trajectory_label(yp, "Яндекс")
            lines.append(f"**Долгосрочная линия (Яндекс):** {traj}\n")

            lines.append("<details><summary>Примеры постов с упоминанием Яндекса</summary>\n")
            for p in yp[:8]:
                preview = p["text_preview"][:300]
                reason = f" — {p['sentiment_reason']}" if p.get("sentiment_reason") else ""
                lines.append(f"- [{p['date'][:10]}] **{p['context']}** | {p['sentiment']}{reason} | Продукты: {', '.join(p['products'])}  ")
                lines.append(f"  > {preview}...\n")
            lines.append("</details>\n")

        # --- КОНКУРЕНТЫ ---
        has_competitors = any(info["competitor_posts"].get(c) for c in COMPETITOR_PATTERNS)
        lines.append(f"### Конкуренты\n")

        if not has_competitors:
            lines.append("Упоминания VK, Сбера или Ozon **не обнаружены**.\n")
        else:
            for comp_name in ["VK", "Сбер", "Ozon"]:
                cp = info["competitor_posts"].get(comp_name, [])
                if not cp:
                    lines.append(f"**{comp_name}:** не упоминается\n")
                    continue

                hosting_count = sum(1 for p in cp if p.get("vk_hosting")) if comp_name == "VK" else 0
                editorial_count = len(cp) - hosting_count

                lines.append(f"**{comp_name}** ({len(cp)} пост(ов)):\n")

                if comp_name == "VK" and hosting_count > 0:
                    lines.append(f"  > Из {len(cp)} постов **{hosting_count}** — кросс-постинг видео на VK Video (хостинг-платформа), **{editorial_count}** — содержательные упоминания.\n")

                sub_prods = defaultdict(int)
                contexts_c = defaultdict(int)
                sentiments_c = defaultdict(int)
                for p in cp:
                    for sp in p["sub_products"]:
                        sub_prods[sp] += 1
                    contexts_c[p["context"]] += 1
                    sentiments_c[p["sentiment"]] += 1

                if sub_prods:
                    lines.append("- Подпродукты: " + ", ".join(
                        f"{sp} ({cnt})" for sp, cnt in sorted(sub_prods.items(), key=lambda x: -x[1])))
                lines.append("- Контекст: " + ", ".join(
                    f"{ctx} ({cnt})" for ctx, cnt in sorted(contexts_c.items(), key=lambda x: -x[1])))
                lines.append("- Тональность: " + ", ".join(
                    f"{s} ({cnt})" for s, cnt in sorted(sentiments_c.items(), key=lambda x: -x[1])))

                traj_c = trajectory_label(cp, comp_name)
                lines.append(f"- **Долгосрочная линия ({comp_name}):** {traj_c}")
                lines.append("")

                non_hosting = [p for p in cp if not p.get("vk_hosting")]
                examples = non_hosting[:3] if non_hosting else cp[:3]
                if examples:
                    lines.append("<details><summary>Примеры постов</summary>\n")
                    for p in examples:
                        preview = p["text_preview"][:300]
                        hosting_note = " [VK Video хостинг]" if p.get("vk_hosting") else ""
                        reason = f" — {p['sentiment_reason']}" if p.get("sentiment_reason") else ""
                        lines.append(f"- [{p['date'][:10]}] **{p['context']}** | {p['sentiment']}{reason}{hosting_note}  ")
                        lines.append(f"  > {preview}...\n")
                    lines.append("</details>\n")

        lines.append("---\n")

    # --- СВОДНАЯ ТАБЛИЦА ---
    lines.append("## Сводная таблица\n")
    lines.append("| Автор / Канал | Постов | Яндекс | Яндекс — линия | VK | VK — линия | Сбер | Сбер — линия | Ozon | Ozon — линия |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")

    for author_key, info in sorted_authors:
        yc = len(info["yandex_posts"])
        yt = trajectory_label(info["yandex_posts"], "Яндекс")
        vk_posts = info["competitor_posts"].get("VK", [])
        vk_c = len(vk_posts)
        vk_t = trajectory_label(vk_posts, "VK")
        sb_posts = info["competitor_posts"].get("Сбер", [])
        sb_c = len(sb_posts)
        sb_t = trajectory_label(sb_posts, "Сбер")
        oz_posts = info["competitor_posts"].get("Ozon", [])
        oz_c = len(oz_posts)
        oz_t = trajectory_label(oz_posts, "Ozon")
        lines.append(f"| {author_key} | {info['total_posts']} | {yc} | {yt} | {vk_c} | {vk_t} | {sb_c} | {sb_t} | {oz_c} | {oz_t} |")

    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Анализ упоминаний брендов в Telegram-каналах")
    parser.add_argument("--dump-posts", action="store_true", help="Выгрузить посты в reports/posts_dump.txt")
    parser.add_argument(
        "--dump-yandex",
        action="store_true",
        help="Выгрузить только посты про Яндекс по каждому автору в reports/yandex_by_author/*.txt",
    )
    parser.add_argument(
        "--allow-keyword-fallback",
        action="store_true",
        help="Разрешить keyword fallback для тональности, если нет LLM-разметки",
    )
    args = parser.parse_args()

    print("Парсинг каналов...")
    all_authors = analyze(allow_keyword_fallback=args.allow_keyword_fallback)

    print(f"Найдено авторов/каналов: {len(all_authors)}")
    for ak, info in all_authors.items():
        y = len(info["yandex_posts"])
        c = sum(len(v) for v in info["competitor_posts"].values())
        print(f"  {ak}: {info['total_posts']} постов, Яндекс={y}, конкуренты={c}")

    if args.dump_posts:
        dump_posts(all_authors)
        if args.dump_yandex:
            dump_yandex_by_author(all_authors)
        return

    if args.dump_yandex:
        dump_yandex_by_author(all_authors)

    print("\nГенерация отчёта...")
    report = generate_report(all_authors)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    out_path = os.path.join(REPORTS_DIR, "report.md")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"Отчёт сохранён: {out_path}")

    period_str = format_period_line(all_authors)
    write_by_author_slices(all_authors, period_str)


if __name__ == "__main__":
    main()
