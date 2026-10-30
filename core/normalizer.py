"""Shared raw-data semantics and provenance-preserving merge."""
from copy import deepcopy
from .contracts import UnifiedNote, EngagementMetrics, FieldPresence, ContentCompleteness
from .security import sanitize_url

def _first(raw, *keys):
    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return None

def _items(value):
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, list):
        return [item if isinstance(item, str) else item.get("url", item.get("url_default", ""))
                for item in value if isinstance(item, (str, dict))]
    return []

def normalize_raw_note(raw, *, keyword=None, rank=None, source_type="search_keyword", data_source="live", is_synthetic=False):
    desc = _first(raw, "desc", "content")
    desc_presence = FieldPresence.NOT_FETCHED if desc is None else FieldPresence.KNOWN_EMPTY if not str(desc).strip() else FieldPresence.VALID
    text = str(desc) if desc is not None else ""
    kind = str(_first(raw, "note_type", "type") or "normal")
    completeness = ContentCompleteness.UNKNOWN if desc is None else DataCleaner.evaluate_completeness(kind, text)
    if raw.get("completeness") in {value.value for value in ContentCompleteness}:
        completeness = ContentCompleteness(raw["completeness"])
    for key in ("desc_presence",):
        if raw.get(key) in {value.value for value in FieldPresence}:
            desc_presence = FieldPresence(raw[key])
    image_value = _first(raw, "image_list", "images")
    video = _first(raw, "video_url")
    images = [sanitize_url(url, media=True) for url in _items(image_value) if url]
    video_url = sanitize_url(video, media=True) if video else ""
    media_presence = (FieldPresence.NOT_FETCHED if image_value is None and video is None
                      else FieldPresence.VALID if images or video_url else FieldPresence.KNOWN_EMPTY)
    if raw.get("media_presence") in {value.value for value in FieldPresence}:
        media_presence = FieldPresence(raw["media_presence"])
    note = UnifiedNote(
        note_id=str(_first(raw, "note_id", "id") or ""), title=str(_first(raw, "title", "display_title") or ""),
        desc=text, note_type=kind, completeness=completeness, desc_presence=desc_presence, media_presence=media_presence,
        author_id=str(_first(raw, "creator_hash", "author_id", "user_id") or ""),
        author_name=str(_first(raw, "author_name", "nickname") or ""),
        metrics=EngagementMetrics.from_raw(
            likes=_first(raw, "liked_count", "likes", "likes_num"), collects=_first(raw, "collected_count", "collects", "collects_num"),
            comments=_first(raw, "comment_count", "comments", "comments_num"), shares=_first(raw, "share_count", "shares", "shares_num")),
        url=sanitize_url(_first(raw, "note_url", "url") or ""), tag_list=_items(_first(raw, "tag_list", "tags")),
        image_list=images, video_url=video_url, published_at=_first(raw, "time", "published_at"),
        is_synthetic=is_synthetic or bool(raw.get("is_synthetic")),
        data_source=raw.get("data_source") or data_source,
    )
    if not note.note_id:
        raise ValueError("Raw note lacks a stable identifier")
    for label in ("likes", "collects", "comments", "shares"):
        if label + "_raw" in raw:
            setattr(note.metrics, "raw_" + label, str(raw[label + "_raw"] or ""))
        if raw.get(label + "_presence") in {value.value for value in FieldPresence}:
            setattr(note.metrics, label + "_presence", FieldPresence(raw[label + "_presence"]))
    if isinstance(raw.get("is_approximate_metric"), bool):
        note.metrics.is_approximate = raw["is_approximate_metric"]
    if keyword is not None:
        note.add_discovery(keyword=keyword, rank=rank, source_type=source_type,
                           discovered_at=raw.get("discovered_at"))
    return note

class DataCleaner:
    @staticmethod
    def deduplicate_notes(notes):
        merged = {}
        for note in notes:
            if note.note_id not in merged:
                merged[note.note_id] = note
                continue
            existing = merged[note.note_id]
            for event in note.discovery_records:
                if event not in existing.discovery_records:
                    existing.discovery_records.append(deepcopy(event))
            for insight in note.insights:
                if insight not in existing.insights:
                    existing.insights.append(deepcopy(insight))
            newer = note.crawled_at >= existing.crawled_at
            complete = note.completeness == ContentCompleteness.FULL and note.desc_presence == FieldPresence.VALID
            upgrade = (complete and (existing.completeness != ContentCompleteness.FULL or newer)
                       or existing.desc_presence not in (FieldPresence.VALID, FieldPresence.KNOWN_EMPTY)
                       and note.desc_presence in (FieldPresence.VALID, FieldPresence.KNOWN_EMPTY))
            if upgrade:
                for field in ("desc", "desc_presence", "completeness", "note_type"):
                    setattr(existing, field, getattr(note, field))
                if note.title:
                    existing.title = note.title
            elif not existing.title and note.title:
                existing.title = note.title
            for label in ("likes", "collects", "comments", "shares"):
                if getattr(note.metrics, label + "_presence") == FieldPresence.VALID:
                    if newer or getattr(existing.metrics, label + "_presence") != FieldPresence.VALID:
                        for field in (label, "raw_" + label, label + "_presence"):
                            setattr(existing.metrics, field, getattr(note.metrics, field))
            existing.metrics.is_approximate = any("万" in getattr(existing.metrics, "raw_" + label) or "w" in getattr(existing.metrics, "raw_" + label).lower()
                                                 for label in ("likes", "collects", "comments", "shares"))
            for tag in note.tag_list:
                if tag and tag not in existing.tag_list:
                    existing.tag_list.append(tag)
            if note.media_presence in (FieldPresence.VALID, FieldPresence.KNOWN_EMPTY) and (newer or existing.media_presence != FieldPresence.VALID):
                existing.image_list = list(note.image_list)
                existing.video_url = note.video_url
                existing.media_presence = note.media_presence
            elif not existing.video_url and note.video_url:
                existing.video_url = note.video_url
                existing.media_presence = FieldPresence.VALID
            for field in ("author_id", "author_name", "url", "published_at"):
                if getattr(note, field) and (newer or not getattr(existing, field)):
                    setattr(existing, field, getattr(note, field))
            existing.crawled_at = max(existing.crawled_at, note.crawled_at)
            existing.is_synthetic = existing.is_synthetic or note.is_synthetic
            if note.is_synthetic:
                existing.data_source = note.data_source
        return list(merged.values())

    @staticmethod
    def evaluate_completeness(note_type, desc):
        if not desc or not desc.strip():
            return ContentCompleteness.EMPTY
        if note_type == "video" and len(desc.strip()) < 40 and "#" in desc:
            return ContentCompleteness.VIDEO_ONLY
        return ContentCompleteness.FULL
