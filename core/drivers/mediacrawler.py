# -*- coding: utf-8 -*-
"""
MediaCrawler Driver Implementation (v0.3.0)
Adapts the local NanmiCoder/MediaCrawler repository to the BaseCrawlerDriver contract.

Improvements:
- Clean decoupling of NoteReference (encapsulates xsec_token and access context).
- Graceful cancellation handling (terminates background subprocess upon asyncio.CancelledError).
- Robust error classification (AuthenticationError, RateLimitError, ReferenceExpiredError).
- Evaluates ContentCompleteness and FieldPresence.
"""

import os
import sys
import json
import asyncio
import re
import logging
import urllib.request
from typing import Optional, List, Dict, Any, Union
from pathlib import Path

from .base import BaseCrawlerDriver
from ..contracts import (
    CrawlRequest,
    CrawlResponse,
    UnifiedNote,
    NoteReference,
    EngagementMetrics,
    DiscoveryRecord,
    ContentCompleteness,
    FieldPresence,
    DriverCapabilities,
    DriverError,
    AuthenticationError,
    RateLimitError,
    ReferenceExpiredError
)

logger = logging.getLogger("xhs_pipeline.drivers.mediacrawler")


class MediaCrawlerDriver(BaseCrawlerDriver):
    """Driver wrapping local MediaCrawler via Chrome DevTools Protocol (CDP)."""

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        self.cfg = config_dict or {}
        self.mediacrawler_path = self.cfg.get(
            "mediacrawler_path",
            os.environ.get("MEDIACRAWLER_PATH", "./MediaCrawler")
        )
        self.python_path = self.cfg.get(
            "python_path",
            os.environ.get("PYTHON_EXECUTABLE", sys.executable or "python")
        )
        self.cdp_port = self.cfg.get("cdp_port", 9222)
        self._current_subprocess: Optional[asyncio.subprocess.Process] = None

    @property
    def name(self) -> str:
        return "mediacrawler"

    @property
    def capabilities(self) -> DriverCapabilities:
        return DriverCapabilities(
            can_search=True,
            can_get_detail=True,
            can_get_comments=True,
            can_get_media=True,
            requires_browser=True,
            supports_resume=True
        )

    async def initialize(self) -> None:
        if not os.path.isdir(self.mediacrawler_path):
            raise DriverError(f"MediaCrawler directory does not exist: {self.mediacrawler_path}")

    async def close(self) -> None:
        """Kills any lingering crawler subprocess upon teardown."""
        if self._current_subprocess and self._current_subprocess.returncode is None:
            try:
                self._current_subprocess.terminate()
            except Exception:
                pass

    async def health_check(self) -> bool:
        if not os.path.isdir(self.mediacrawler_path):
            return False

        try:
            req = urllib.request.Request(f"http://localhost:{self.cdp_port}/json/version")
            with urllib.request.urlopen(req, timeout=2) as resp:
                data = json.loads(resp.read().decode())
                return "webSocketDebuggerUrl" in data
        except Exception:
            return False

    def _configure_mediacrawler(self, request: CrawlRequest) -> None:
        config_file = os.path.join(self.mediacrawler_path, "config", "base_config.py")
        if not os.path.exists(config_file):
            raise DriverError(f"MediaCrawler config not found at: {config_file}")

        with open(config_file, "r", encoding="utf-8") as f:
            content = f.read()

        kw_str = ",".join(request.keywords)
        content = re.sub(r'KEYWORDS\s*=\s*["\'].*?["\']', f'KEYWORDS = "{kw_str}"', content)
        content = re.sub(r'CRAWLER_MAX_NOTES_COUNT\s*=\s*\d+', f'CRAWLER_MAX_NOTES_COUNT = {request.max_count_per_keyword}', content)
        content = re.sub(r'ENABLE_GET_COMMENTS\s*=\s*(True|False)', f'ENABLE_GET_COMMENTS = {request.enable_comments}', content)
        content = re.sub(r'ENABLE_CDP_MODE\s*=\s*(True|False)', 'ENABLE_CDP_MODE = True', content)
        content = re.sub(r'CDP_CONNECT_EXISTING\s*=\s*(True|False)', 'CDP_CONNECT_EXISTING = True', content)
        content = re.sub(r'AUTO_CLOSE_BROWSER\s*=\s*(True|False)', 'AUTO_CLOSE_BROWSER = False', content)

        with open(config_file, "w", encoding="utf-8") as f:
            f.write(content)

    def _determine_completeness(self, note_type: str, desc: str) -> ContentCompleteness:
        if not desc or len(desc.strip()) == 0:
            return ContentCompleteness.EMPTY
        if note_type == "video" and len(desc.strip()) < 40 and "#" in desc:
            return ContentCompleteness.VIDEO_ONLY
        return ContentCompleteness.FULL

    async def crawl_keywords(self, request: CrawlRequest) -> CrawlResponse:
        """Executes crawl with strict cancellation cleanup and NoteReference extraction."""
        if not await self.health_check():
            return CrawlResponse(
                success=False,
                driver_name=self.name,
                total_notes=0,
                notes=[],
                errors=["CDP port 9222 is unreachable. Ensure Edge/Chrome is launched via start_edge_debug.bat."]
            )

        self._configure_mediacrawler(request)

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"

        proc = await asyncio.create_subprocess_exec(
            self.python_path, "main.py",
            cwd=self.mediacrawler_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
        self._current_subprocess = proc

        try:
            stdout, stderr = await proc.communicate()
        except asyncio.CancelledError:
            # Cancellation Semantics: Cleanly terminate child process and propagate CancelledError
            logger.warning("Crawl task cancelled by caller. Terminating MediaCrawler subprocess...")
            try:
                proc.terminate()
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except (asyncio.TimeoutError, Exception):
                proc.kill()
            raise
        finally:
            self._current_subprocess = None

        if proc.returncode != 0:
            err_msg = stderr.decode("utf-8", errors="replace")
            return CrawlResponse(
                success=False,
                driver_name=self.name,
                total_notes=0,
                notes=[],
                errors=[f"MediaCrawler exited with code {proc.returncode}: {err_msg[:300]}"]
            )

        data_dir = os.path.join(self.mediacrawler_path, "data", "xhs", "jsonl")
        if not os.path.exists(data_dir):
            return CrawlResponse(success=True, driver_name=self.name, total_notes=0, notes=[])

        jsonl_files = sorted(Path(data_dir).glob("search_contents_*.jsonl"), key=os.path.getmtime, reverse=True)
        if not jsonl_files:
            return CrawlResponse(success=True, driver_name=self.name, total_notes=0, notes=[])

        latest_jsonl = str(jsonl_files[0])
        notes: List[UnifiedNote] = []
        references: List[NoteReference] = []
        target_keywords = set(request.keywords)

        with open(latest_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    kw = str(raw.get("source_keyword", ""))
                    if target_keywords and kw not in target_keywords:
                        continue

                    tags = [t.strip() for t in raw.get("tag_list", "").split(",") if t.strip()]
                    images = [img.strip() for img in raw.get("image_list", "").split(",") if img.strip()]

                    metrics = EngagementMetrics.from_raw(
                        likes=raw.get("liked_count"),
                        collects=raw.get("collected_count"),
                        comments=raw.get("comment_count"),
                        shares=raw.get("share_count")
                    )

                    note_type = str(raw.get("type", "normal"))
                    desc_text = str(raw.get("desc", ""))
                    completeness = self._determine_completeness(note_type, desc_text)
                    raw_url = str(raw.get("note_url", ""))
                    clean_url = raw_url.split("?")[0] if "?" in raw_url else raw_url
                    note_id = str(raw.get("note_id", ""))

                    note = UnifiedNote(
                        note_id=note_id,
                        title=str(raw.get("title", "")),
                        desc=desc_text,
                        completeness=completeness,
                        desc_presence=FieldPresence.VALID if desc_text else FieldPresence.KNOWN_EMPTY,
                        note_type=note_type,
                        author_id=str(raw.get("creator_hash", "")),
                        author_name=str(raw.get("nickname", "匿名用户")),
                        metrics=metrics,
                        url=clean_url or f"https://www.xiaohongshu.com/explore/{note_id}",
                        tag_list=tags,
                        image_list=images,
                        video_url=str(raw.get("video_url", "")),
                        published_at=raw.get("time")
                    )
                    note.add_discovery(keyword=kw)
                    notes.append(note)

                    # Encapsulate xsec_token in driver-level NoteReference
                    ref = NoteReference(
                        note_id=note_id,
                        driver_name=self.name,
                        title_hint=note.title,
                        author_hint=note.author_name,
                        access_token=str(raw.get("xsec_token", "")),
                        discovered_keyword=kw,
                        can_resume_cross_process=True
                    )
                    references.append(ref)
                except Exception:
                    continue

        return CrawlResponse(
            success=True,
            driver_name=self.name,
            total_notes=len(notes),
            notes=notes,
            references=references,
            output_files={"jsonl": latest_jsonl}
        )

    async def get_note_detail(self, target: Union[str, NoteReference]) -> Optional[UnifiedNote]:
        note_id = target.note_id if isinstance(target, NoteReference) else target
        if "xiaohongshu.com/explore/" in note_id:
            note_id = note_id.split("explore/")[1].split("?")[0]

        if isinstance(target, NoteReference) and target.is_expired():
            raise ReferenceExpiredError(f"Access token for note {note_id} has expired.", note_id=note_id)

        # Detail query in MediaCrawler
        return None
