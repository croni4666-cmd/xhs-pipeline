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
        """Finds existing file for note_id with exact frontmatter or exact name matching."""
        if not os.path.exists(self.vault_dir):
            return None

        exact_path = os.path.join(self.vault_dir, f"XHS_{note_id}.md")
        if os.path.isfile(exact_path):
            return exact_path

        target_token = f'note_id: "{note_id}"'
        prefix = f"XHS_{note_id}_"
        candidates = []
        for p in Path(self.vault_dir).glob("XHS_*.md"):
            if not p.is_file():
                continue
            if p.name == f"XHS_{note_id}.md":
                return str(p)
            if p.name.startswith(prefix) or p.name.startswith(f"XHS_{note_id}."):
                candidates.append(p)

        # 1. Match by frontmatter note_id
        for p in candidates:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    head = f.read(1024)
                if target_token in head:
                    return str(p)
            except Exception:
                continue

        # 2. Match legacy files without YAML frontmatter
        for p in candidates:
            try:
                with open(p, "r", encoding="utf-8") as f:
                    head = f.read(1024)
                if "note_id: " in head and target_token not in head:
                    continue
                return str(p)
            except Exception:
                continue

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

                begin_tag = "<!-- BEGIN_USER_NOTES -->"
                end_tag = "<!-- END_USER_NOTES -->"

                if begin_tag in old_content:
                    start_pos = old_content.find(begin_tag) + len(begin_tag)
                    if end_tag in old_content[start_pos:]:
                        end_offset = old_content[start_pos:].find(end_tag)
                        inside_notes = old_content[start_pos:start_pos + end_offset]
                        after_notes = old_content[start_pos + end_offset + len(end_tag):]
                        preserved_user_notes = inside_notes.strip()
                        if after_notes.strip():
                            preserved_user_notes = (preserved_user_notes + "\n\n" + after_notes.strip()).strip()
                    else:
                        preserved_user_notes = old_content[start_pos:].strip()

                elif self.USER_NOTES_HEADER in old_content:
                    # In our layout, the designated header is after "### 🏷️ 关联标签"
                    tag_pos = old_content.rfind("### 🏷️ 关联标签")
                    if tag_pos != -1:
                        sub = old_content[tag_pos:]
                        header_offset = sub.find(self.USER_NOTES_HEADER)
                        if header_offset != -1:
                            preserved_user_notes = sub[header_offset + len(self.USER_NOTES_HEADER):].strip()
                        else:
                            preserved_user_notes = old_content.split(self.USER_NOTES_HEADER)[-1].strip()
                    else:
                        fm_end = old_content.find("\n---\n", 3)
                        search_start = fm_end + 5 if fm_end != -1 else 0
                        header_pos = old_content.find(self.USER_NOTES_HEADER, search_start)
                        if header_pos != -1:
                            preserved_user_notes = old_content[header_pos + len(self.USER_NOTES_HEADER):].strip()
                        else:
                            preserved_user_notes = old_content.rsplit(self.USER_NOTES_HEADER, 1)[1].strip()

                else:
                    # Legacy or user file without designated header
                    if old_content.strip():
                        preserved_user_notes = old_content.strip()

                # Clean untouched default placeholder
                default_hint = "在此记录您针对该笔记的研究心得、拆解要点或仿写灵感（重新导出时将受到永久保护，不会被覆盖）。"
                if preserved_user_notes and default_hint in preserved_user_notes:
                    cleaned = preserved_user_notes.replace(default_hint, "").replace(">", "").strip()
                    if not cleaned:
                        preserved_user_notes = ""

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
        notes_section = preserved_user_notes if preserved_user_notes else '> 在此记录您针对该笔记的研究心得、拆解要点或仿写灵感（重新导出时将受到永久保护，不会被覆盖）。'
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
<!-- BEGIN_USER_NOTES -->
{notes_section}
<!-- END_USER_NOTES -->
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
