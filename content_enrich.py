"""
Обогащение текста поста для LLM-разметки тональности: транскрипты YouTube, если в посте есть ссылка.

Без установленного youtube-transcript-api функции работают в no-op режиме.
"""

import re
from typing import Any, Dict, List, Optional

# Кэш на время процесса — повторные прогоны analyze не дергают YouTube заново
_TRANSCRIPT_CACHE: Dict[str, Optional[str]] = {}

YOUTUBE_VIDEO_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})"
)


def extract_youtube_video_ids(urls: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for u in urls:
        m = YOUTUBE_VIDEO_RE.search(u)
        if m:
            vid = m.group(1)
            if vid not in seen:
                seen.add(vid)
                out.append(vid)
    return out


def _fetch_transcript_youtube(video_id: str) -> Optional[str]:
    if video_id in _TRANSCRIPT_CACHE:
        return _TRANSCRIPT_CACHE[video_id]
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except ImportError:
        _TRANSCRIPT_CACHE[video_id] = None
        return None
    try:
        api = YouTubeTranscriptApi()
        t = api.fetch(video_id, languages=["ru", "en"])
        text = " ".join(s.text for s in t.snippets)
        _TRANSCRIPT_CACHE[video_id] = text
        return text
    except Exception:
        _TRANSCRIPT_CACHE[video_id] = None
        return None


def enrich_text_for_llm(caption: str, urls: List[str]) -> Dict[str, Any]:
    """
    Возвращает caption + при наличии YouTube — полный транскрипт (для оценки тона по содержанию ролика).
    """
    caption = caption or ""
    if not urls or not any("youtu" in u.lower() or "youtube.com" in u.lower() for u in urls):
        return {
            "text": caption[:12000],
            "transcript_enriched": False,
            "youtube_video_ids": [],
            "transcript_note": None,
        }
    ids = extract_youtube_video_ids(urls)
    if not ids:
        return {
            "text": caption[:12000],
            "transcript_enriched": False,
            "youtube_video_ids": [],
            "transcript_note": None,
        }

    parts: List[str] = [caption.strip()]
    notes: List[str] = []
    enriched = False

    for vid in ids[:3]:
        tr = _fetch_transcript_youtube(vid)
        if tr:
            enriched = True
            parts.append(f"\n\n--- Транскрипт YouTube ({vid}) ---\n{tr.strip()}")
        else:
            notes.append(f"youtube:{vid}:транскрипт недоступен")

    text = "\n".join(parts)[:12000]
    return {
        "text": text,
        "transcript_enriched": enriched,
        "youtube_video_ids": ids,
        "transcript_note": "; ".join(notes) if notes else None,
    }
