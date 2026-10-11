"""Public task metadata and transaction-safe export checkpoints."""
import hashlib
import json
import os
from pathlib import Path
import time
from .contracts import CrawlTaskManifest, NoteReference, StageStatus, PipelineError
from .persistence import atomic_write_json, file_lock
from .security import safe_component

class CheckpointStore:
    def __init__(self, state_dir="./data/checkpoints"):
        self.state_dir = str(Path(state_dir).resolve())
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)

    def _get_manifest_path(self, task_id):
        return str(Path(self.state_dir) / f"manifest_{safe_component(task_id)}.json")

    def _get_ref_path(self, task_id):
        return str(Path(self.state_dir) / f"references_{safe_component(task_id)}.json")

    def _get_commit_log_path(self, task_id):
        return str(Path(self.state_dir) / f"commits_{safe_component(task_id)}.json")

    def _atomic_write_json(self, file_path, data):
        with file_lock(file_path + ".lock"):
            atomic_write_json(file_path, data)

    def save_task_manifest(self, manifest):
        path = self._get_manifest_path(manifest.task_id)
        self._atomic_write_json(path, manifest.to_dict())
        return path

    def load_task_manifest(self, task_id):
        path = self._get_manifest_path(task_id)
        if not os.path.exists(path):
            return None
        with file_lock(path + ".lock"):
            with open(path, encoding="utf-8") as stream:
                return CrawlTaskManifest(**json.load(stream))

    def save_references(self, task_id, refs):
        path = self._get_ref_path(task_id)
        payload = []
        for ref in refs:
            needs_re_search = bool(ref.access_token or ref.access_context)
            payload.append({
                "note_id": ref.note_id, "driver_name": ref.driver_name,
                "title_hint": ref.title_hint, "author_hint": ref.author_hint,
                "access_token": "", "access_context": {},
                "discovered_keyword": ref.discovered_keyword, "rank": ref.rank,
                "created_at": ref.created_at, "expires_at": ref.expires_at,
                "can_resume_cross_process": ref.can_resume_cross_process and not needs_re_search,
                "is_stale": ref.is_stale or needs_re_search,
            })
        self._atomic_write_json(path, payload)
        return path

    def load_references(self, task_id):
        path = self._get_ref_path(task_id)
        if not os.path.exists(path):
            return [], []
        with file_lock(path + ".lock"):
            with open(path, encoding="utf-8") as stream:
                payload = json.load(stream)
            if any(entry.get("access_token") or entry.get("access_context") for entry in payload):
                cleaned = []
                for entry in payload:
                    clean = {key: value for key, value in entry.items()
                             if key in {"note_id", "driver_name", "title_hint", "author_hint", "discovered_keyword", "rank", "created_at", "expires_at", "can_resume_cross_process", "is_stale"}}
                    if entry.get("access_token") or entry.get("access_context"):
                        clean.update(can_resume_cross_process=False, is_stale=True)
                    clean.update(access_token="", access_context={})
                    cleaned.append(clean)
                self._atomic_write_json(path, cleaned)
                payload = cleaned
        valid, expired = [], []
        for entry in payload:
            stale = bool(entry.get("is_stale") or entry.get("access_token") or entry.get("access_context"))
            ref = NoteReference(
                note_id=entry["note_id"], driver_name=entry["driver_name"],
                title_hint=entry.get("title_hint", ""), author_hint=entry.get("author_hint", ""),
                access_token="", access_context={},
                discovered_keyword=entry.get("discovered_keyword", ""), rank=entry.get("rank"),
                created_at=entry.get("created_at", time.time()), expires_at=entry.get("expires_at"),
                can_resume_cross_process=entry.get("can_resume_cross_process", False) and not stale,
                is_stale=stale,
            )
            if not ref.can_resume_cross_process or ref.is_expired():
                ref.is_stale = True
                expired.append(ref)
            else:
                valid.append(ref)
        return valid, expired

    def _read_commits(self, path, quarantine=False):
        if not os.path.exists(path):
            return {}
        try:
            with open(path, encoding="utf-8") as stream:
                payload = json.load(stream)
            if not isinstance(payload, dict):
                raise ValueError("Invalid commit log")
            return payload
        except (ValueError, OSError):
            if quarantine:
                os.replace(path, path + f".corrupt_{time.time_ns()}")
            return {}

    def commit_export(self, task_id, note_id, file_path, source_hash, artifact_hash="", render_input_hash=""):
        safe_component(note_id)
        path = self._get_commit_log_path(task_id)
        with file_lock(path + ".lock"):
            commits = self._read_commits(path, quarantine=True)
            commits[note_id] = {
                "file_path": file_path, "source_hash": source_hash, "content_hash": source_hash,
                "artifact_hash": artifact_hash, "render_input_hash": render_input_hash,
                "committed_at": time.time(), "status": StageStatus.EXPORTED.value,
            }
            self._atomic_write_json(path, commits)

    def get_commit(self, task_id, note_id):
        path = self._get_commit_log_path(task_id)
        with file_lock(path + ".lock"):
            return self._read_commits(path).get(note_id)

    def reconcile_after_crash(self, task_id, vault_dir, notes):
        from .storage import ObsidianExporter
        exporter = ObsidianExporter(vault_dir)
        stages = {}
        for note in notes:
            note.stage_status = StageStatus.CRAWLED
            try:
                with exporter.card_lock(note.note_id):
                    path = exporter._find_existing_file(note.note_id)
                    if path is not None:
                        parts = exporter.read_parts(path, note.note_id)
                        # Reconstruct the complete generated bytes from current inputs.
                        # User-only edits may change artifact hash without changing facts.
                        if exporter.is_current(parts, note):
                            digest = hashlib.sha256(parts.original).hexdigest()
                            previous = self.get_commit(task_id, note.note_id)
                            if (not previous or previous.get("artifact_hash") != digest
                                    or previous.get("render_input_hash") != note.get_render_input_hash()
                                    or previous.get("file_path") != path):
                                self.commit_export(task_id, note.note_id, path, note.content_hash,
                                                   digest, note.get_render_input_hash())
                            note.stage_status = StageStatus.EXPORTED
            except (OSError, PipelineError):
                # Export will surface conflicts/read failures while preserving bytes.
                pass
            stages[note.note_id] = note.stage_status
        return stages
