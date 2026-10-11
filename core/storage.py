"""Identity checked cards with separate generated and human regions."""
import csv
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from .contracts import FieldPresence, StageStatus, PipelineError
from .persistence import atomic_write_bytes, file_lock
from .security import safe_component, sanitize_url

class ExportConflictError(PipelineError):
    """Existing identity or annotation boundaries require human resolution."""

def sanitize_csv_cell(value):
    if isinstance(value, str) and value and (value[0] in "\t\r\n" or value.lstrip().startswith(("=", "+", "-", "@"))):
        return "'" + value
    return value

def _machine_text(value):
    return str(value).replace("<!--", "&lt;!--").replace("-->", "--&gt;")

def _frontmatter(text):
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---\n", 4)
    if end == -1:
        raise ExportConflictError("Incomplete card frontmatter")
    fields = {}
    for line in text[4:end].splitlines():
        key, sep, value = line.partition(":")
        if not sep or line.startswith((" ", "\t")):
            continue
        if key in fields:
            raise ExportConflictError("Duplicate card identity fields")
        try:
            fields[key] = json.loads(value.strip())
        except ValueError:
            fields[key] = value.strip()
    return fields

@dataclass
class CardParts:
    metadata: dict
    generated: str
    user_notes: str
    original: bytes
    modern: bool

class ObsidianExporter:
    USER_NOTES_HEADER = "## 💡 个人笔记与批注 (User Notes & Annotations)"
    USER_BEGIN = "<!-- BEGIN_USER_NOTES -->"
    USER_END = "<!-- END_USER_NOTES -->"
    GENERATED_BEGIN = "<!-- XHS_GENERATED_BEGIN -->"
    GENERATED_END = "<!-- XHS_GENERATED_END -->"

    def __init__(self, vault_dir):
        self.vault_dir = str(Path(vault_dir).resolve())
        Path(self.vault_dir).mkdir(parents=True, exist_ok=True)

    def card_lock(self, note_id):
        safe_component(note_id)
        name = ".xhs-" + hashlib.sha256(note_id.encode()).hexdigest() + ".lock"
        return file_lock(Path(self.vault_dir) / name)

    def _read(self, path):
        with open(path, "r", encoding="utf-8", newline="") as stream:
            return stream.read()

    def _find_existing_file(self, note_id):
        safe_component(note_id)
        exact = Path(self.vault_dir) / f"XHS_{note_id}.md"
        if exact.exists():
            if exact.is_symlink() or not exact.is_file():
                raise ExportConflictError("Card path is not a regular local file")
            identity = _frontmatter(self._read(str(exact)).replace("\r\n", "\n")).get("note_id")
            if identity is not None and identity != note_id:
                raise ExportConflictError("Card filename and identity disagree")
            return str(exact)
        matches = []
        for path in Path(self.vault_dir).glob("XHS_*.md"):
            if path.is_symlink() or not path.is_file() or not path.name.startswith(f"XHS_{note_id}_"):
                continue
            identity = _frontmatter(self._read(str(path)).replace("\r\n", "\n")).get("note_id")
            if identity == note_id:
                matches.append(str(path))
        if len(matches) > 1:
            raise ExportConflictError("Multiple cards claim the same note identity")
        return matches[0] if matches else None

    def read_parts(self, path, note_id):
        text = self._read(path)
        normalized = text.replace("\r\n", "\n")
        metadata = _frontmatter(normalized)
        if metadata.get("note_id", note_id) != note_id:
            raise ExportConflictError("Existing card has a conflicting note identity")
        modern = metadata.get("card_schema") == "xhs_card_v2"
        if self.USER_BEGIN in text or self.USER_END in text:
            if text.count(self.USER_BEGIN) != 1 or text.count(self.USER_END) != 1:
                raise ExportConflictError("Ambiguous annotation markers; original preserved")
            start, end = text.index(self.USER_BEGIN), text.index(self.USER_END)
            if start >= end:
                raise ExportConflictError("Reversed annotation markers; original preserved")
            generated = text[:start + len(self.USER_BEGIN)] + "\n"
            notes = text[start + len(self.USER_BEGIN):end]
            if notes.startswith("\r\n"):
                notes = notes[2:]
            elif notes.startswith("\n"):
                notes = notes[1:]
            if notes.endswith("\r\n"):
                notes = notes[:-2]
            elif notes.endswith("\n"):
                notes = notes[:-1]
            trailing = text[end + len(self.USER_END):]
            if trailing.strip():
                notes += trailing
        elif modern:
            raise ExportConflictError("Missing annotation boundary; original preserved")
        else:
            marker = text.find(self.USER_NOTES_HEADER)
            notes = text[marker + len(self.USER_NOTES_HEADER):] if marker >= 0 else text
            generated = ""
        if modern and (normalized.count(self.GENERATED_BEGIN) != 1 or normalized.count(self.GENERATED_END) != 1):
            raise ExportConflictError("Ambiguous generated region; original preserved")
        return CardParts(metadata, generated, notes, text.encode("utf-8"), modern)

    def _format_metric_display(self, raw, presence):
        labels = {FieldPresence.NOT_FETCHED: "[未采集]", FieldPresence.UNSUPPORTED: "[不支持]", FieldPresence.FETCH_FAILED: "[采集失败]"}
        return labels.get(presence, raw or "0")

    def generated_prefix(self, note, crawled_at=None):
        safe_component(note.note_id)
        fact = note.fact
        meta = {
            "card_schema": "xhs_card_v2", "note_id": note.note_id, "title": _machine_text(note.title),
            "platform": "小红书", "author": _machine_text(note.author_name),
            "keywords": [_machine_text(d.keyword) for d in note.discovery_records if d.keyword],
            "content_hash": note.content_hash, "render_input_hash": note.get_render_input_hash(),
            "completeness": note.completeness.value, "desc_presence": note.desc_presence.value,
            "media_presence": note.media_presence.value, "note_type": _machine_text(note.note_type),
            "url": _machine_text(sanitize_url(note.url)), "is_synthetic": note.is_synthetic, "data_source": _machine_text(note.data_source),
            "crawled_at": note.crawled_at if crawled_at is None else crawled_at,
            "tags": [_machine_text(tag) for tag in note.tag_list],
        }
        for label in ("likes", "collects", "comments", "shares"):
            meta[label + "_num"] = getattr(note.metrics, label)
            meta[label + "_raw"] = _machine_text(getattr(note.metrics, "raw_" + label))
            meta[label + "_presence"] = getattr(note.metrics, label + "_presence").value
        header = "---\n" + "\n".join(f"{key}: {json.dumps(value, ensure_ascii=False)}" for key, value in meta.items()) + "\n---\n"
        body = (f"\n# {_machine_text(note.title)}\n\n> **作者**: {_machine_text(note.author_name)}\n"
                f"> **数据来源**: {_machine_text(note.data_source)} | **模拟数据**: {note.is_synthetic}\n"
                f"> **原文链接**: {_machine_text(sanitize_url(note.url))}\n\n### 📝 笔记事实正文\n\n{note.get_sanitized_desc()}\n")
        for insight in note.insights:
            body += (f"\n### 🧠 内容特征与分析洞察\n模型: {_machine_text(insight.model_name)} | 提示词: {_machine_text(insight.prompt_version)}\n"
                     f"模式: {_machine_text(insight.hook_archetype)} | 置信度: {_machine_text(insight.confidence_score)}\n")
            body += "\n".join("> " + _machine_text(quote) for quote in insight.evidence_snippets) + "\n"
        body += "\n### 🏷️ 关联标签\n" + " ".join("#" + _machine_text(tag) for tag in note.tag_list) + "\n"
        if fact.image_list or fact.video_url:
            body += "\n### 媒体链接\n" + "\n".join(_machine_text(url) for url in fact.image_list + ([fact.video_url] if fact.video_url else [])) + "\n"
        return header + self.GENERATED_BEGIN + "\n" + body + self.GENERATED_END + "\n\n" + self.USER_NOTES_HEADER + "\n" + self.USER_BEGIN + "\n"

    def is_current(self, parts, note):
        return (parts.modern and parts.metadata.get("note_id") == note.note_id
                and parts.generated == self.generated_prefix(note, parts.metadata.get("crawled_at")))

    def export_note(self, note):
        with self.card_lock(note.note_id):
            existing = self._find_existing_file(note.note_id)
            path = existing or str(Path(self.vault_dir) / f"XHS_{note.note_id}.md")
            parts = self.read_parts(path, note.note_id) if existing else None
            if parts and self.is_current(parts, note):
                note.stage_status = StageStatus.EXPORTED
                return path, hashlib.sha256(parts.original).hexdigest()
            notes = parts.user_notes if parts else "> 在此记录研究心得；生成区更新会保留本区域。"
            data = (self.generated_prefix(note) + notes + "\n" + self.USER_END + "\n").encode("utf-8")
            if parts:
                backup = path + "." + hashlib.sha256(parts.original).hexdigest()[:16] + ".bak"
                if not os.path.exists(backup):
                    atomic_write_bytes(backup, parts.original)
                if Path(path).read_bytes() != parts.original:
                    raise ExportConflictError("Card changed during update; original not replaced")
            atomic_write_bytes(path, data)
            note.stage_status = StageStatus.EXPORTED
            return path, hashlib.sha256(data).hexdigest()

    def export_all(self, notes):
        return [self.export_note(note)[0] for note in notes]

class CsvExporter:
    def __init__(self, output_path):
        self.output_path = output_path
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    def export(self, notes):
        fields = ["note_id", "keywords", "title", "desc", "completeness", "desc_presence", "media_presence", "note_type", "author_name",
                  "likes_num", "likes_raw", "likes_presence", "collects_num", "collects_raw", "collects_presence",
                  "comments_num", "comments_raw", "comments_presence", "shares_num", "shares_raw", "shares_presence",
                  "is_approximate_metric", "url", "tags", "images", "video_url", "stage_status", "content_hash", "is_synthetic", "data_source"]
        import io
        output = io.StringIO(newline="")
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for note in notes:
            writer.writerow({k: sanitize_csv_cell(v) for k, v in note.to_dict().items()})
        atomic_write_bytes(self.output_path, output.getvalue().encode("utf-8-sig"))
        return self.output_path
