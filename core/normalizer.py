# -*- coding: utf-8 -*-
"""
Data Cleaner & Normalizer Component (v0.3.0)
Single-responsibility component handling:
- Content deduplication and discovery trail merging.
- Field presence and completeness validation.
- Prompt injection neutralization and text sanitization.
"""

from typing import List, Dict, Any, Optional
from .contracts import (
    UnifiedNote,
    DiscoveryRecord,
    FieldPresence,
    ContentCompleteness,
    EngagementMetrics
)


class DataCleaner:
    """Cleans, normalizes, and deduplicates Xiaohongshu note data."""

    @staticmethod
    def deduplicate_notes(notes: List[UnifiedNote]) -> List[UnifiedNote]:
        """
        Deduplicates notes by note_id.
        When a note is discovered via multiple keywords, merges all DiscoveryRecords
        into the single note instance, preserving all research trails.
        """
        dedup_map: Dict[str, UnifiedNote] = {}

        for note in notes:
            if note.note_id not in dedup_map:
                dedup_map[note.note_id] = note
            else:
                existing = dedup_map[note.note_id]
                # Merge discovery records
                for rec in note.discovery_records:
                    existing.add_discovery(
                        keyword=rec.keyword,
                        rank=rec.rank,
                        source_type=rec.source_type
                    )
                # Merge tags and images if existing were incomplete
                for t in note.tag_list:
                    if t not in existing.tag_list:
                        existing.tag_list.append(t)
                for img in note.image_list:
                    if img not in existing.image_list:
                        existing.image_list.append(img)
                # If existing had incomplete description but new one has full, upgrade it
                if existing.desc_presence != FieldPresence.VALID and note.desc_presence == FieldPresence.VALID:
                    existing.desc = note.desc
                    existing.desc_presence = FieldPresence.VALID
                    existing.completeness = note.completeness

        return list(dedup_map.values())

    @staticmethod
    def evaluate_completeness(note_type: str, desc: str) -> ContentCompleteness:
        """Evaluates textual content completeness."""
        if not desc or len(desc.strip()) == 0:
            return ContentCompleteness.EMPTY
        if note_type == "video" and len(desc.strip()) < 40 and "#" in desc:
            return ContentCompleteness.VIDEO_ONLY
        return ContentCompleteness.FULL
