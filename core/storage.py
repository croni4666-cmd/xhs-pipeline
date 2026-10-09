# -*- coding: utf-8 -*-
"""
Storage & Exporters Module (v0.3.1)
Single-responsibility export components implementing:
- Atomic file writes (write to unique temp, flush, fsync, atomic replace) to prevent crash corruption.
- SHA-256 content hash tracking for consistency reconciliation.
- Non-destructive Obsidian updates preserving user manual annotations (protection against read failures).
- Formula injection protection in CSV exports.
- Explicit FieldPresence rendering (distinguishing uncollected from zero).
"""

import os
import csv
import re
import time
import logging
import hashlib
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

from .contracts import (
    UnifiedNote,
    FieldPresence,
    ContentCompleteness,
    StageStatus
)

logger = logging.getLogger("xhs_pipeline.storage")


def sanitize_csv_cell(val: Any) -> Any:
    """Neutralizes spreadsheet formula injection (=, +, -, @, \t, \r)."""
    if isinstance(val, str) and val and val[0] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + val
    return val


class ObsidianExporter:
    """Exports notes to Obsidian Markdown cards with atomic write and idempotent reconciliation."""

    USER_NOTES_HEADER = "## 💡 个人笔记与批注 (User Notes & Annotations)"

    def __init__(self, vault_dir: str):
        self.vault_dir = vault_dir
        Path(self.vault_dir).mkdir(parents=True, exist_ok=True)

    def _format_metric_display(self, raw_val: str, presence: FieldPresence) -> str:
        """Formats metrics explicitly distinguishing uncollected from zero."""
        if presence == FieldPresence.NOT_FETCHED:
            return "[未采集]"
        elif presence == FieldPresence.UNSUPPORTED:
            return "[不支持]"
        elif presence == FieldPresence.FETCH_FAILED:
            return "[采集失败]"
        return raw_val if raw_val else "0"

    def _find_existing_file(self, note_id: str) -> Optional[str]:
        """Finds existing file for note_id with exact prefix matching to prevent prefix collision."""
        if not os.path.exists(self.vault_dir):
            return None
        safe_id = re.escape(note_id)
        pattern = re.compile(rf"^XHS_{safe_id}(_.*)?\.md$")
        for p in Path(self.vault_dir).iterdir():
            if p.is_file() and pattern.match(p.name):
                return str(p)
        return None

    def export_note(self, note: UnifiedNote) -> Tuple[str, str]:
        """
        Atomically exports a single note to Obsidian.
        Returns:
            Tuple[file_path, written_content_hash]
        """
        safe_title = note.get_sanitized_title()

        # Idempotent file naming: find existing card with exact note_id match
        existing_file = self._find_existing_file(note.note_id)
        if existing_file:
            file_path = existing_file
        else:
            filename = f"XHS_{note.note_id}_{safe_title[:30]}.md" if safe_title else f"XHS_{note.note_id}.md"
            file_path = os.path.join(self.vault_dir, filename)

        # 1. Preserve existing user manual notes under designated header
        preserved_user_notes = ""
        if os.path.exists(file_path):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    old_content = f.read()
                    if self.USER_NOTES_HEADER in old_content:
                        # Extract from the LAST occurrence to avoid false matches in source description
                        preserved_user_notes = old_content.rsplit(self.USER_NOTES_HEADER, 1)[1]
            except OSError as e:
                # If existing file cannot be read, never silently overwrite human annotations!
                logger.error(f"Cannot read existing card {file_path} to preserve annotations: {e}")
                raise OSError(f"Existing card read failure, aborted overwrite to preserve human annotations: {e}")

        # 2. Render tags and keywords
        tags_yaml = "\n".join([f"  - {t}" for t in note.tag_list if t])
        if not tags_yaml:
            tags_yaml = f"  - 小红书\n  - {note.primary_keyword or '未分类'}"

        kw_tags = " ".join([f"[[{d.keyword}]]" for d in note.discovery_records if d.keyword])

        # 3. Render insights if available
        insights_md = ""
        if note.insights:
            insights_md = "\n### 🧠 内容特征与分析洞察 (Analytical Insights)\n"
            for ins in note.insights:
                quote = ins.evidence_snippets[0] if ins.evidence_snippets else "无"
                insights_md += f"""- **分析模型**: `{ins.model_name}` (提示词: `{ins.prompt_version}`)
- **前置钩子模式**: `{ins.hook_archetype}` | **置信度**: `{ins.confidence_score}`
- **核心论点证据引用**:
  > "{quote}"
"""

        # 4. Render metrics with explicit presence semantics
        likes_display = self._format_metric_display(note.metrics.raw_likes, note.metrics.likes_presence)
        collects_display = self._format_metric_display(note.metrics.raw_collects, note.metrics.collects_presence)
        comments_display = self._format_metric_display(note.metrics.raw_comments, note.metrics.comments_presence)

        # 5. Build Markdown card with LF line endings
        md_content = f"""---
note_id: "{note.note_id}"
title: "{note.title.replace('"', "'")}"
platform: 小红书
keywords: {[d.keyword for d in note.discovery_records if d.keyword]}
author: "{note.author_name}"
likes_num: {note.metrics.likes if note.metrics.likes is not None else 'null'}
likes_raw: "{note.metrics.raw_likes}"
likes_presence: "{note.metrics.likes_presence.value}"
collects_num: {note.metrics.collects if note.metrics.collects is not None else 'null'}
collects_raw: "{note.metrics.raw_collects}"
comments_num: {note.metrics.comments if note.metrics.comments is not None else 'null'}
comments_raw: "{note.metrics.raw_comments}"
comments_presence: "{note.metrics.comments_presence.value}"
completeness: "{note.completeness.value}"
desc_presence: "{note.desc_presence.value}"
note_type: "{note.note_type}"
url: "{note.url}"
crawled_at: "{note.crawled_at}"
content_hash: "{note.content_hash}"
tags:
{tags_yaml}
---

# {note.title}

> **作者**: [[{note.author_name}]] | **研究主题**: {kw_tags}  
> **互动指标**: ❤️ {likes_display} 点赞 | ⭐ {collects_display} 收藏 | 💬 {comments_display} 评论  
> **数据完整度**: `{note.completeness.value}` (正文状态: `{note.desc_presence.value}`) | **原文链接**: [在小红书查看]({note.url})

---

### 📝 笔记事实正文

{note.get_sanitized_desc()}
{insights_md}
---

### 🏷️ 关联标签
{' '.join(['#' + t for t in note.tag_list])}

---

{self.USER_NOTES_HEADER}
{preserved_user_notes if preserved_user_notes else '\n> 在此记录您针对该笔记的研究心得、拆解要点或仿写灵感（重新导出时将受到永久保护，不会被覆盖）。\n'}
"""

        # 6. Atomic Write: write exact bytes to unique .tmp file, then replace
        temp_file = f"{file_path}.{os.getpid()}_{time.time_ns()}.tmp"
        md_bytes = md_content.encode("utf-8")
        with open(temp_file, "wb") as f:
            f.write(md_bytes)
            f.flush()
            os.fsync(f.fileno())

        os.replace(temp_file, file_path)
        written_hash = hashlib.sha256(md_bytes).hexdigest()
        note.stage_status = StageStatus.EXPORTED

        return file_path, written_hash

    def export_all(self, notes: List[UnifiedNote]) -> List[str]:
        written_paths = []
        for n in notes:
            path, _ = self.export_note(n)
            written_paths.append(path)
        return written_paths


class CsvExporter:
    """Exports notes to a structured CSV table with typed metrics and formula injection protection."""

    def __init__(self, output_path: str):
        self.output_path = output_path
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)

    def export(self, notes: List[UnifiedNote]) -> str:
        fieldnames = [
            "note_id", "keywords", "title", "desc", "completeness", "desc_presence",
            "note_type", "author_name",
            "likes_num", "likes_raw", "likes_presence",
            "collects_num", "collects_raw", "collects_presence",
            "comments_num", "comments_raw", "comments_presence",
            "shares_num", "shares_raw", "shares_presence",
            "is_approximate_metric", "url", "tags", "images", "stage_status", "content_hash"
        ]
        with open(self.output_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for n in notes:
                raw_dict = n.to_dict()
                sanitized_dict = {k: sanitize_csv_cell(v) for k, v in raw_dict.items()}
                writer.writerow(sanitized_dict)
        return self.output_path
