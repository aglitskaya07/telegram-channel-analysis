#!/usr/bin/env python3
"""
Парсинг публичных ТГ-каналов через Telethon.
Выгружает посты за заданный период в JSON, совместимый с Telegram Desktop Export.

Использование:
  pip install telethon
  python3 scrape.py --channels mobiledevnews android_broadcast --since 2025-10-01

При первом запуске — авторизация через номер телефона + код из Telegram.
Сессия сохраняется в файл .session (повторный вход не нужен).
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone, timedelta

try:
    from telethon import TelegramClient
    from telethon.tl.types import (
        MessageEntityBold, MessageEntityItalic, MessageEntityCode,
        MessageEntityPre, MessageEntityUrl, MessageEntityTextUrl,
        MessageEntityMention, MessageEntityHashtag, MessageEntityBotCommand,
        MessageEntitySpoiler, MessageEntityBlockquote, MessageEntityCustomEmoji,
        MessageEntityStrike, MessageEntityUnderline,
        ReactionEmoji, ReactionCustomEmoji, ReactionPaid,
    )
except ImportError:
    print("Telethon не установлен. Выполните: pip install telethon")
    sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORTS_DIR = os.path.join(BASE_DIR, "exports")
SESSION_FILE = os.path.join(BASE_DIR, "tg_session")
CREDENTIALS_FILE = os.path.join(BASE_DIR, ".tg_credentials.json")

ENTITY_TYPE_MAP = {
    MessageEntityBold: "bold",
    MessageEntityItalic: "italic",
    MessageEntityCode: "code",
    MessageEntityPre: "pre",
    MessageEntityUrl: "link",
    MessageEntityTextUrl: "text_link",
    MessageEntityMention: "mention",
    MessageEntityHashtag: "hashtag",
    MessageEntityBotCommand: "bot_command",
    MessageEntitySpoiler: "spoiler",
    MessageEntityBlockquote: "blockquote",
    MessageEntityCustomEmoji: "custom_emoji",
    MessageEntityStrike: "strikethrough",
    MessageEntityUnderline: "underline",
}


def build_text_and_entities(raw_text, entities):
    """Convert Telethon entities to Desktop Export format (mixed text array + text_entities list)."""
    if not raw_text:
        return "", []

    if not entities:
        return raw_text, [{"type": "plain", "text": raw_text}]

    sorted_ents = sorted(entities, key=lambda e: e.offset)
    text_parts = []
    text_entities = []
    pos = 0

    for ent in sorted_ents:
        if ent.offset > pos:
            plain = raw_text[pos:ent.offset]
            text_parts.append(plain)
            text_entities.append({"type": "plain", "text": plain})

        ent_text = raw_text[ent.offset:ent.offset + ent.length]
        ent_type = ENTITY_TYPE_MAP.get(type(ent), "unknown")

        obj = {"type": ent_type, "text": ent_text}
        if isinstance(ent, MessageEntityTextUrl) and ent.url:
            obj["href"] = ent.url
        if isinstance(ent, MessageEntityPre) and getattr(ent, "language", None):
            obj["language"] = ent.language
        if isinstance(ent, MessageEntityCustomEmoji):
            obj["document_id"] = str(ent.document_id)

        text_parts.append(obj)
        text_entities.append(obj.copy())
        pos = ent.offset + ent.length

    if pos < len(raw_text):
        tail = raw_text[pos:]
        text_parts.append(tail)
        text_entities.append({"type": "plain", "text": tail})

    return text_parts, text_entities


def format_reactions(msg_reactions):
    """Convert Telethon reactions to Desktop Export format."""
    if not msg_reactions or not msg_reactions.results:
        return []
    result = []
    for r in msg_reactions.results:
        reaction = r.reaction
        if isinstance(reaction, ReactionEmoji):
            result.append({"type": "emoji", "count": r.count, "emoji": reaction.emoticon})
        elif isinstance(reaction, ReactionCustomEmoji):
            result.append({"type": "custom_emoji", "count": r.count, "document_id": str(reaction.document_id)})
        elif isinstance(reaction, ReactionPaid):
            result.append({"type": "paid", "count": r.count})
    return result


def format_date(dt):
    if not dt:
        return None
    local_dt = dt.astimezone()
    return local_dt.strftime("%Y-%m-%dT%H:%M:%S")


def format_unixtime(dt):
    if not dt:
        return None
    return str(int(dt.timestamp()))


async def fetch_comments(client, entity, msg_id, max_comments=100):
    """Fetch comments (replies) for a channel post."""
    comments = []
    try:
        async for reply in client.iter_messages(
            entity, reply_to=msg_id, limit=max_comments
        ):
            if not reply.message:
                continue
            sender_name = "Unknown"
            if reply.sender:
                sender_name = getattr(reply.sender, "title", None) or \
                    " ".join(filter(None, [
                        getattr(reply.sender, "first_name", ""),
                        getattr(reply.sender, "last_name", ""),
                    ])) or "Unknown"

            text_parts, text_entities = build_text_and_entities(
                reply.message, reply.entities
            )

            comment_obj = {
                "id": reply.id,
                "date": format_date(reply.date),
                "date_unixtime": format_unixtime(reply.date),
                "from": sender_name,
                "from_id": f"user{reply.sender_id}" if reply.sender_id else None,
                "text": text_parts if isinstance(text_parts, list) else text_parts,
                "text_entities": text_entities,
            }

            reactions = format_reactions(reply.reactions)
            if reactions:
                comment_obj["reactions"] = reactions

            comments.append(comment_obj)
    except Exception as e:
        if "CHANNEL_PRIVATE" not in str(e) and "MSG_ID_INVALID" not in str(e):
            print(f"    Ошибка при загрузке комментариев к посту {msg_id}: {e}")
    comments.sort(key=lambda c: c["id"])
    return comments


async def scrape_channel(client, channel_username, since_date, until_date,
                         with_comments=False, max_comments_per_post=100):
    """Scrape a single channel and return data in Desktop Export format."""
    print(f"\n--- Загрузка канала @{channel_username} ---")

    try:
        entity = await client.get_entity(channel_username)
    except Exception as e:
        print(f"  Ошибка: не удалось найти канал @{channel_username}: {e}")
        return None

    channel_name = getattr(entity, "title", channel_username)
    channel_id = entity.id

    messages = []
    count = 0
    skipped = 0
    comments_total = 0

    async for msg in client.iter_messages(
        entity,
        offset_date=until_date,
    ):
        msg_date = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
        if msg_date < since_date:
            break

        if not msg.message:
            skipped += 1
            continue

        text_parts, text_entities = build_text_and_entities(msg.message, msg.entities)

        message_obj = {
            "id": msg.id,
            "type": "message",
            "date": format_date(msg.date),
            "date_unixtime": format_unixtime(msg.date),
            "from": channel_name,
            "from_id": f"channel{channel_id}",
            "text": text_parts if isinstance(text_parts, list) else text_parts,
            "text_entities": text_entities,
        }

        if msg.edit_date:
            message_obj["edited"] = format_date(msg.edit_date)
            message_obj["edited_unixtime"] = format_unixtime(msg.edit_date)

        if msg.post_author:
            message_obj["author"] = msg.post_author

        if msg.fwd_from:
            fwd = msg.fwd_from
            if fwd.from_name:
                message_obj["forwarded_from"] = fwd.from_name
            elif fwd.from_id:
                try:
                    fwd_entity = await client.get_entity(fwd.from_id)
                    fwd_name = getattr(fwd_entity, "title", None) or \
                               getattr(fwd_entity, "first_name", "Unknown")
                    message_obj["forwarded_from"] = fwd_name
                    message_obj["forwarded_from_id"] = f"channel{fwd_entity.id}"
                except Exception:
                    message_obj["forwarded_from"] = "Unknown"

        reactions = format_reactions(msg.reactions)
        if reactions:
            message_obj["reactions"] = reactions

        if msg.views is not None:
            message_obj["views"] = msg.views
        if msg.forwards is not None:
            message_obj["forwards"] = msg.forwards

        if with_comments and getattr(msg, "replies", None) and msg.replies.replies > 0:
            comments = await fetch_comments(
                client, entity, msg.id, max_comments_per_post
            )
            if comments:
                message_obj["comments"] = comments
                message_obj["comments_count"] = len(comments)
                comments_total += len(comments)

        messages.append(message_obj)
        count += 1

        if count % 100 == 0:
            print(f"  Загружено {count} сообщений...")

    messages.sort(key=lambda m: m["id"])

    result = {
        "name": channel_name,
        "type": "public_channel",
        "id": channel_id,
        "messages": messages,
    }

    comment_info = f", {comments_total} комментариев" if with_comments else ""
    print(f"  Готово: {len(messages)} постов с текстом ({skipped} без текста пропущено{comment_info})")
    return result


def load_credentials():
    if os.path.exists(CREDENTIALS_FILE):
        with open(CREDENTIALS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_credentials(api_id, api_hash):
    with open(CREDENTIALS_FILE, "w") as f:
        json.dump({"api_id": api_id, "api_hash": api_hash}, f)


async def main_async(args):
    os.makedirs(EXPORTS_DIR, exist_ok=True)

    api_id = args.api_id
    api_hash = args.api_hash

    if not api_id or not api_hash:
        saved = load_credentials()
        api_id = api_id or saved.get("api_id")
        api_hash = api_hash or saved.get("api_hash")

    if not api_id or not api_hash:
        print("=" * 60)
        print("Для работы нужны api_id и api_hash от Telegram.")
        print("Получите их на https://my.telegram.org")
        print("(раздел API development tools)")
        print("=" * 60)
        api_id = input("Введите api_id: ").strip()
        api_hash = input("Введите api_hash: ").strip()

    save_credentials(api_id, api_hash)

    client = TelegramClient(SESSION_FILE, int(api_id), api_hash)
    await client.start()
    print(f"Авторизация успешна: {(await client.get_me()).first_name}")

    since_date = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    until_date = None
    if args.until:
        # Верхняя граница YYYY-MM-DD включает весь этот календарный день (UTC): exclusive = +1 день
        d = datetime.strptime(args.until, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        until_date = d + timedelta(days=1)
    else:
        until_date = datetime.now(timezone.utc)

    for channel in args.channels:
        channel = channel.lstrip("@").split("/")[-1]
        data = await scrape_channel(
            client, channel, since_date, until_date,
            with_comments=args.comments,
            max_comments_per_post=args.max_comments,
        )
        if data:
            out_path = os.path.join(EXPORTS_DIR, f"{channel}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=1)
            print(f"  Сохранено: {out_path}")

    await client.disconnect()
    print(f"\nГотово! JSON-файлы в папке: {EXPORTS_DIR}")


def main():
    parser = argparse.ArgumentParser(
        description="Парсинг публичных ТГ-каналов через Telethon"
    )
    parser.add_argument(
        "--channels", nargs="+", required=True,
        help="Юзернеймы каналов (без @), например: mobiledevnews android_broadcast"
    )
    parser.add_argument(
        "--since", required=True,
        help="Дата начала (YYYY-MM-DD), например: 2025-10-01"
    )
    parser.add_argument(
        "--until", default=None,
        help="Дата конца (YYYY-MM-DD), по умолчанию — сегодня"
    )
    parser.add_argument(
        "--comments", action="store_true", default=False,
        help="Собирать комментарии к постам"
    )
    parser.add_argument(
        "--max-comments", type=int, default=100, dest="max_comments",
        help="Макс. кол-во комментариев на пост (по умолчанию 100)"
    )
    parser.add_argument("--api-id", dest="api_id", default=None, help="Telegram api_id")
    parser.add_argument("--api-hash", dest="api_hash", default=None, help="Telegram api_hash")

    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
