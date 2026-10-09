# -*- coding: utf-8 -*-
"""
Checkpoint & Task State Store (v0.3.0)
Coordinates persistent checkpoints, task manifests, and crash recovery.

Solves:
1. Cross-process resumption of NoteReference access contexts.
2. Crash recovery reconciliation (file exists on disk vs committed state).
3. Complete study reproducibility by preserving CrawlTaskManifest.
"""

import os
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

    def save_task_manifest(self, manifest: CrawlTaskManifest) -> str:
        """Saves search criteria, sample counts, and version metadata for study reproduction."""
        path = self._get_manifest_path(manifest.task_id)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest.to_dict(), f, indent=2, ensure_ascii=False)
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
        """
        path = self._get_ref_path(task_id)
        payload = [
            {
                "note_id": r.note_id,
                "driver_name": r.driver_name,
                "title_hint": r.title_hint,
                "author_hint": r.author_hint,
                "access_token": r.access_token,
                "access_context": r.access_context,
                "discovered_keyword": r.discovered_keyword,
                "rank": r.rank,
                "created_at": r.created_at,
                "expires_at": r.expires_at,
                "can_resume_cross_process": r.can_resume_cross_process,
                "is_stale": r.is_stale
            }
            for r in refs
        ]
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
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
            # Evaluate staleness
            if not ref.can_resume_cross_process:
                ref.is_stale = True
                expired_refs.append(ref)
            elif ref.is_expired():
                expired_refs.append(ref)
            else:
                valid_refs.append(ref)

        return valid_refs, expired_refs

    def commit_export(self, task_id: str, note_id: str, file_path: str, content_hash: str) -> None:
        """
        Atomically commits the exported card state AFTER file write has completed.
        Ensures consistency between filesystem and checkpoint state.
        """
        log_path = self._get_commit_log_path(task_id)
        commits: Dict[str, Any] = {}
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    commits = json.load(f)
            except Exception:
                commits = {}

        commits[note_id] = {
            "file_path": file_path,
            "content_hash": content_hash,
            "committed_at": time.time(),
            "status": StageStatus.EXPORTED.value
        }

        temp_log = log_path + ".tmp"
        with open(temp_log, "w", encoding="utf-8") as f:
            json.dump(commits, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_log, log_path)

    def reconcile_after_crash(self, task_id: str, vault_dir: str, notes: List[UnifiedNote]) -> Dict[str, StageStatus]:
        """
        Reconciles crash inconsistency:
        If file exists on disk and content_hash matches, marks as EXPORTED.
        If file exists but commit is missing, recovers and registers commit without re-writing!
        """
        log_path = self._get_commit_log_path(task_id)
        commits = {}
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    commits = json.load(f)
            except Exception:
                pass

        stage_map = {}
        for note in notes:
            if note.note_id in commits:
                stage_map[note.note_id] = StageStatus.EXPORTED
                note.stage_status = StageStatus.EXPORTED
                continue

            # Check if file exists on disk
            matches = list(Path(vault_dir).glob(f"XHS_{note.note_id}*.md"))
            if matches:
                existing_file = str(matches[0])
                try:
                    with open(existing_file, "r", encoding="utf-8") as f:
                        text = f.read()
                        if f'content_hash: "{note.content_hash}"' in text:
                            # Matches expected content, commit and recover!
                            self.commit_export(task_id, note.note_id, existing_file, note.content_hash)
                            stage_map[note.note_id] = StageStatus.EXPORTED
                            note.stage_status = StageStatus.EXPORTED
                            continue
                except Exception:
                    pass

            stage_map[note.note_id] = note.stage_status

        return stage_map
