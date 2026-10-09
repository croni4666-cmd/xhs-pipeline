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
            # Sensitive auth secrets/cookies are NEVER written to plaintext JSON disk!
            safe_context = {}
            if isinstance(r.access_context, dict):
                for k, v in r.access_context.items():
                    k_lower = str(k).lower()
                    if any(secret_kw in k_lower for secret_kw in ("cookie", "token", "auth", "secret", "key", "password", "session")):
                        continue
                    safe_context[k] = v

            payload.append({
                "note_id": r.note_id,
                "driver_name": r.driver_name,
                "title_hint": r.title_hint,
                "author_hint": r.author_hint,
                "access_token": "",  # Never persist plaintext secrets to disk sink
                "access_context": safe_context,
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
        artifact_hash: str = "",
        render_input_hash: str = ""
    ) -> None:
        """
        Atomically commits the exported card state AFTER file write has completed.
        Ensures consistency between filesystem and checkpoint state.
        Records source_hash, artifact_hash, and render_input_hash.
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
            "render_input_hash": render_input_hash,
            "committed_at": time.time(),
            "status": StageStatus.EXPORTED.value
        }
        self._atomic_write_json(log_path, commits)

    def get_commit(self, task_id: str, note_id: str) -> Optional[Dict[str, Any]]:
        log_path = self._get_commit_log_path(task_id)
        if os.path.exists(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    commits = json.load(f)
                return commits.get(note_id)
            except Exception:
                pass
        return None

    def reconcile_after_crash(self, task_id: str, vault_dir: str, notes: List[UnifiedNote]) -> Dict[str, StageStatus]:
        """
        Reconciles crash inconsistency with strict verification:
        1. If committed: verifies file exists, matches disk artifact hash, and matches render inputs.
        2. If uncommitted: verifies exact frontmatter, heading, and body in fact section.
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
                committed_render = commit_info.get("render_input_hash", "")

                # Verify file actually exists on disk
                if card_file and os.path.isfile(card_file):
                    try:
                        disk_bytes = Path(card_file).read_bytes()
                        disk_artifact_hash = hashlib.sha256(disk_bytes).hexdigest()
                    except OSError:
                        disk_bytes = b""
                        disk_artifact_hash = ""

                    # Verify disk file hash matches artifact_hash if recorded
                    if committed_artifact and disk_artifact_hash != committed_artifact:
                        # File was tampered, truncated, or replaced!
                        note.stage_status = StageStatus.CRAWLED
                        stage_map[note.note_id] = StageStatus.CRAWLED
                        continue

                    # Verify source content hasn't changed
                    if committed_source != note.content_hash and committed_artifact != note.content_hash:
                        note.stage_status = StageStatus.CRAWLED
                        stage_map[note.note_id] = StageStatus.CRAWLED
                        continue

                    # Verify render inputs (metrics, discovery, insights, author, etc.) haven't changed
                    if committed_render and hasattr(note, "get_render_input_hash") and committed_render != note.get_render_input_hash():
                        note.stage_status = StageStatus.CRAWLED
                        stage_map[note.note_id] = StageStatus.CRAWLED
                        continue

                    stage_map[note.note_id] = StageStatus.EXPORTED
                    note.stage_status = StageStatus.EXPORTED
                    continue
                else:
                    # File was deleted on disk! Cannot mark as EXPORTED
                    note.stage_status = StageStatus.CRAWLED
                    stage_map[note.note_id] = StageStatus.CRAWLED
                    continue

            # 2. Check uncommitted file on disk
            target_token = f'note_id: "{note.note_id}"'
            found_file = None
            if os.path.exists(vault_dir):
                for p in Path(vault_dir).glob("XHS_*.md"):
                    if p.is_file() and (p.name == f"XHS_{note.note_id}.md" or p.name.startswith(f"XHS_{note.note_id}_")):
                        try:
                            text = p.read_text(encoding="utf-8")
                            # Verify frontmatter note_id and content_hash
                            if target_token in text and f'content_hash: "{note.content_hash}"' in text:
                                # Verify document structure: title heading, fact section, and body placement
                                if f"# {note.title}" in text and "### 📝 笔记事实正文" in text:
                                    body_part = text.split("### 📝 笔记事实正文", 1)[1]
                                    fact_body = body_part.split("## 📝 我的批注与研究笔记", 1)[0] if "## 📝 我的批注与研究笔记" in body_part else body_part
                                    if (not note.desc) or (note.get_sanitized_desc() in fact_body):
                                        found_file = str(p)
                                        disk_hash = hashlib.sha256(p.read_bytes()).hexdigest()
                                        render_hash = note.get_render_input_hash() if hasattr(note, "get_render_input_hash") else ""
                                        self.commit_export(
                                            task_id=task_id,
                                            note_id=note.note_id,
                                            file_path=found_file,
                                            source_hash=note.content_hash,
                                            artifact_hash=disk_hash,
                                            render_input_hash=render_hash
                                        )
                                        stage_map[note.note_id] = StageStatus.EXPORTED
                                        note.stage_status = StageStatus.EXPORTED
                                        break
                        except Exception:
                            pass

            if found_file:
                continue

            note.stage_status = StageStatus.CRAWLED
            stage_map[note.note_id] = note.stage_status

        return stage_map
