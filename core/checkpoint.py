# -*- coding: utf-8 -*-
"""
Checkpoint & Task State Store (v0.3.1)
Coordinates persistent checkpoints, task manifests, and crash recovery.

Solves:
1. Cross-process resumption of NoteReference access contexts with zero credential leakage.
2. Crash recovery reconciliation (verifying disk file existence, body integrity & content hash).
3. Complete study reproducibility by preserving CrawlTaskManifest atomically.
"""

import os
import re
import json
import time
import hashlib
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from .contracts import (
    CrawlTaskManifest,
    NoteReference,
    StageStatus,
    UnifiedNote
)


class CheckpointStore:
    """Manages stage state persistence and cross-process crash recovery."""

    def __init__(self, state_dir: str = "./data/checkpoints"):
        self.state_dir = state_dir
        Path(self.state_dir).mkdir(parents=True, exist_ok=True)

    def _get_manifest_path(self, task_id: str) -> str:
        return os.path.join(self.state_dir, f"manifest_{task_id}.json")

    def _get_ref_path(self, task_id: str) -> str:
        return os.path.join(self.state_dir, f"references_{task_id}.json")

    def _get_commit_log_path(self, task_id: str) -> str:
        return os.path.join(self.state_dir, f"commits_{task_id}.json")

    def _atomic_write_json(self, file_path: str, data: Any):
        """Atomically writes JSON using unique temp files, flush, fsync, and replace."""
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        temp_file = f"{file_path}.{os.getpid()}_{time.time_ns()}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_file, file_path)

    def save_task_manifest(self, manifest: CrawlTaskManifest) -> str:
        """Atomically saves search criteria, sample counts, and version metadata."""
        path = self._get_manifest_path(manifest.task_id)
        self._atomic_write_json(path, manifest.to_dict())
        return path

    def load_task_manifest(self, task_id: str) -> Optional[CrawlTaskManifest]:
        path = self._get_manifest_path(task_id)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return CrawlTaskManifest(**data)

    def save_references(self, task_id: str, refs: List[NoteReference]) -> str:
        """
        Saves query references with driver access context.
        Explicitly tracks can_resume_cross_process and expiration timestamp.
        Zero credential leakage: Non-resumable tokens and cookies are NEVER saved to disk.
        """
        path = self._get_ref_path(task_id)
        payload = []
        for r in refs:
            is_resumable = bool(r.can_resume_cross_process and not r.is_expired())
            payload.append({
                "note_id": r.note_id,
                "driver_name": r.driver_name,
                "title_hint": r.title_hint,
                "author_hint": r.author_hint,
                "access_token": r.access_token if is_resumable else "",
                "access_context": r.access_context if is_resumable else {},
                "discovered_keyword": r.discovered_keyword,
                "rank": r.rank,
                "created_at": r.created_at,
                "expires_at": r.expires_at,
                "can_resume_cross_process": r.can_resume_cross_process,
                "is_stale": r.is_stale
            })
        self._atomic_write_json(path, payload)
        return path

    def load_references(self, task_id: str) -> Tuple[List[NoteReference], List[NoteReference]]:
        """
        Loads saved references across process restart.
        Returns:
            Tuple[valid_references, expired_references]
        """
        path = self._get_ref_path(task_id)
        if not os.path.exists(path):
            return [], []

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        valid_refs = []
        expired_refs = []
        now = time.time()

        for d in data:
            ref = NoteReference(
                note_id=d["note_id"],
                driver_name=d["driver_name"],
                title_hint=d.get("title_hint", ""),
                author_hint=d.get("author_hint", ""),
                access_token=d.get("access_token", ""),
                access_context=d.get("access_context", {}),
                discovered_keyword=d.get("discovered_keyword", ""),
                rank=d.get("rank"),
                created_at=d.get("created_at", now),
                expires_at=d.get("expires_at"),
                can_resume_cross_process=d.get("can_resume_cross_process", True),
                is_stale=d.get("is_stale", False)
            )
            if not ref.can_resume_cross_process:
                ref.is_stale = True
                expired_refs.append(ref)
            elif ref.is_expired():
                expired_refs.append(ref)
            else:
                valid_refs.append(ref)

        return valid_refs, expired_refs

    def commit_export(
        self,
        task_id: str,
        note_id: str,
        file_path: str,
        source_hash: str,
        artifact_hash: str = ""
    ) -> None:
        """
        Atomically commits the exported card state AFTER file write has completed.
        Ensures consistency between filesystem and checkpoint state.
        Records both source_hash and artifact_hash.
        """
        log_path = self._get_commit_log_path(task_id)
        commits: Dict[str, Any] = {}
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    commits = json.load(f)
            except Exception:
                try:
                    os.replace(log_path, f"{log_path}.corrupt_{int(time.time())}")
                except Exception:
                    pass
                commits = {}

        commits[note_id] = {
            "file_path": file_path,
            "source_hash": source_hash,
            "content_hash": source_hash,
            "artifact_hash": artifact_hash,
            "committed_at": time.time(),
            "status": StageStatus.EXPORTED.value
        }
        self._atomic_write_json(log_path, commits)

    def reconcile_after_crash(self, task_id: str, vault_dir: str, notes: List[UnifiedNote]) -> Dict[str, StageStatus]:
        """
        Reconciles crash inconsistency with strict verification:
        1. If committed: verifies file exists on disk and source content hash matches.
        2. If uncommitted: checks for existing valid card on disk, verifies body integrity and content hash.
        3. Prevents prefix collisions (exact note_id matching).
        """
        log_path = self._get_commit_log_path(task_id)
        commits: Dict[str, Any] = {}
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    commits = json.load(f)
            except Exception:
                pass

        stage_map = {}
        for note in notes:
            # 1. Check committed state
            if note.note_id in commits:
                commit_info = commits[note.note_id]
                card_file = commit_info.get("file_path", "")
                committed_source = commit_info.get("source_hash") or commit_info.get("content_hash", "")
                committed_artifact = commit_info.get("artifact_hash", "")

                # Verify file actually exists on disk
                if card_file and os.path.exists(card_file):
                    # Verify content hasn't changed since commit
                    if committed_source == note.content_hash or (committed_artifact and committed_artifact == note.content_hash):
                        stage_map[note.note_id] = StageStatus.EXPORTED
                        note.stage_status = StageStatus.EXPORTED
                        continue
                    else:
                        # Content was updated, needs re-export
                        note.stage_status = StageStatus.CRAWLED
                        stage_map[note.note_id] = StageStatus.CRAWLED
                        continue
                else:
                    # File was deleted on disk! Cannot mark as EXPORTED
                    note.stage_status = StageStatus.CRAWLED
                    stage_map[note.note_id] = StageStatus.CRAWLED
                    continue

            # 2. Check uncommitted file on disk (with exact prefix regex to prevent collisions)
            safe_id = re.escape(note.note_id)
            id_pattern = re.compile(rf"^XHS_{safe_id}(_.*)?\.md$")
            matches = [
                str(p) for p in Path(vault_dir).iterdir()
                if p.is_file() and id_pattern.match(p.name)
            ] if os.path.exists(vault_dir) else []

            if matches:
                existing_file = matches[0]
                try:
                    with open(existing_file, "r", encoding="utf-8") as f:
                        text = f.read()

                    # Verify header, content_hash and body integrity (not just a stub content_hash line)
                    has_hash = f'content_hash: "{note.content_hash}"' in text
                    has_body = (not note.desc) or (note.desc in text)
                    has_frontmatter = text.startswith("---") and "\n---" in text[3:]

                    if has_hash and has_body and has_frontmatter:
                        # Valid file recovered from crash! Register commit and mark EXPORTED
                        self.commit_export(task_id, note.note_id, existing_file, note.content_hash)
                        stage_map[note.note_id] = StageStatus.EXPORTED
                        note.stage_status = StageStatus.EXPORTED
                        continue
                except Exception:
                    pass

            note.stage_status = StageStatus.CRAWLED
            stage_map[note.note_id] = note.stage_status

        return stage_map
