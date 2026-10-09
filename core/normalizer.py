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
                # Merge discovery records with original timestamps
                for rec in note.discovery_records:
                    existing.add_discovery(
                        keyword=rec.keyword,
                        rank=rec.rank,
                        source_type=rec.source_type,
                        discovered_at=rec.discovered_at
                    )
                # Upgrade title if new one is more informative
                if (not existing.title or len(note.title) > len(existing.title)) and note.title:
                    existing.title = note.title
                # Upgrade desc and completeness: never downgrade from FULL to TRUNCATED or EMPTY!
                should_upgrade_desc = False
                if existing.completeness != ContentCompleteness.FULL:
                    if note.completeness == ContentCompleteness.FULL:
                        should_upgrade_desc = True
                    elif existing.desc_presence != FieldPresence.VALID and note.desc_presence == FieldPresence.VALID:
                        should_upgrade_desc = True
                    elif note.completeness != ContentCompleteness.EMPTY and len(note.desc.strip()) > len(existing.desc.strip()):
                        should_upgrade_desc = True
                else:
                    if note.completeness == ContentCompleteness.FULL and len(note.desc.strip()) > len(existing.desc.strip()):
                        should_upgrade_desc = True

                if should_upgrade_desc:
                    existing.desc = note.desc
                    existing.desc_presence = note.desc_presence
                    existing.completeness = note.completeness
                # Upgrade video_url if previously missing
                if not existing.video_url and note.video_url:
                    existing.video_url = note.video_url
                # Upgrade engagement metrics if existing had uncollected metrics
                if existing.metrics.likes_presence != FieldPresence.VALID and note.metrics.likes_presence == FieldPresence.VALID:
                    existing.metrics.likes = note.metrics.likes
                    existing.metrics.raw_likes = note.metrics.raw_likes
                    existing.metrics.likes_presence = FieldPresence.VALID
                if existing.metrics.collects_presence != FieldPresence.VALID and note.metrics.collects_presence == FieldPresence.VALID:
                    existing.metrics.collects = note.metrics.collects
                    existing.metrics.raw_collects = note.metrics.raw_collects
                    existing.metrics.collects_presence = FieldPresence.VALID
                if existing.metrics.comments_presence != FieldPresence.VALID and note.metrics.comments_presence == FieldPresence.VALID:
                    existing.metrics.comments = note.metrics.comments
                    existing.metrics.raw_comments = note.metrics.raw_comments
                    existing.metrics.comments_presence = FieldPresence.VALID
                if existing.metrics.shares_presence != FieldPresence.VALID and note.metrics.shares_presence == FieldPresence.VALID:
                    existing.metrics.shares = note.metrics.shares
                    existing.metrics.raw_shares = note.metrics.raw_shares
                    existing.metrics.shares_presence = FieldPresence.VALID
                # Merge tags and images if existing were incomplete
                for t in note.tag_list:
                    if t and t not in existing.tag_list:
                        existing.tag_list.append(t)
                for img in note.image_list:
                    if img and img not in existing.image_list:
                        existing.image_list.append(img)

        return list(dedup_map.values())

    @staticmethod
    def evaluate_completeness(note_type: str, desc: str) -> ContentCompleteness:
        """Evaluates textual content completeness."""
        if not desc or len(desc.strip()) == 0:
            return ContentCompleteness.EMPTY
        if note_type == "video" and len(desc.strip()) < 40 and "#" in desc:
            return ContentCompleteness.VIDEO_ONLY
        return ContentCompleteness.FULL
