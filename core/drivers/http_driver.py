# -*- coding: utf-8 -*-
"""
HttpCrawlerDriver (v0.3.0)
Offline HTTP-contract simulation with access context separation and NoteReference generation.
"""

import time
import json
import hashlib
import logging
from typing import Optional, List, Dict, Any, Union

try:
    import httpx
except ImportError:
    httpx = None

from .base import BaseCrawlerDriver
from ..contracts import (
    CrawlRequest,
    CrawlResponse,
    UnifiedNote,
    NoteReference,
    EngagementMetrics,
    ContentCompleteness,
    FieldPresence,
    DriverCapabilities,
    DriverError,
    AuthenticationError,
    RateLimitError,
    ReferenceExpiredError,
    sanitize_note_url,
    CapabilityUnsupportedError
)

logger = logging.getLogger("xhs_pipeline.drivers.http")


class HttpCrawlerDriver(BaseCrawlerDriver):
    """
    Offline simulation of a browserless driver contract; performs no platform requests.
    Generates NoteReference query references with encapsulated access tokens.
    """

    is_synthetic = True

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.base_url = self.config.get("http_endpoint", "https://api.xiaohongshu.com/gateway")
        self.api_key = self.config.get("api_key", "")
        self.timeout = float(self.config.get("timeout_seconds", 15.0))
        self.client: Optional[Any] = None
        self._is_initialized = False

    @property
    def name(self) -> str:
        return "http"

    @property
    def capabilities(self) -> DriverCapabilities:
        return DriverCapabilities(
            can_search=True,
            can_get_detail=True,
            can_get_comments=False,
            can_get_media=True,
            requires_browser=False,
            supports_resume=True
        )

    async def initialize(self) -> None:
        if httpx is None:
            raise DriverError("httpx package is required for HttpCrawlerDriver.")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=self.timeout,
            follow_redirects=True
        )
        self._is_initialized = True

    async def close(self) -> None:
        if self.client and not self.client.is_closed:
            await self.client.aclose()
        self._is_initialized = False

    async def health_check(self) -> bool:
        if not self._is_initialized or self.client is None or self.client.is_closed:
            await self.initialize()
        return self.client is not None and not self.client.is_closed

    def _parse_raw_note(self, item, keyword=""):
        from ..normalizer import normalize_raw_note
        return normalize_raw_note(item, keyword=keyword or None,
                                  source_type="synthetic_http", data_source="synthetic_simulation", is_synthetic=True)

    async def crawl_keywords(self, request: CrawlRequest) -> CrawlResponse:
        if not (request.allow_synthetic or self.config.get("allow_synthetic", False)):
            raise CapabilityUnsupportedError("HTTP demonstration data requires explicit allow_synthetic consent")
        await self.health_check()
        notes: List[UnifiedNote] = []
        references: List[NoteReference] = []
        errors: List[str] = []
        is_partial = False

        for kw in request.keywords:
            try:
                kw_hash = hashlib.sha256(kw.encode("utf-8")).hexdigest()[:8]
                synthetic_items = [
                    {
                        "note_id": f"http_{kw_hash}_{i}",
                        "title": f"【HTTP驱动】{kw} 深度指南 第{i+1}期",
                        "desc": f"这是离线生成的 {kw} 演示正文，用于测试 HTTP 驱动契约，并非平台采集结果。",
                        "liked_count": f"{(i+1)*2.3:.1f}万",
                        "collected_count": f"{(i+1)*1200}",
                        "comment_count": f"{(i+1)*88}",
                        "share_count": "56",
                        "tags": f"{kw},真实体验,推荐",
                        "nickname": f"探店达人_{i}"
                    }
                    for i in range(min(5, request.max_count_per_keyword))
                ]
                for idx, item in enumerate(synthetic_items, 1):
                    n = self._parse_raw_note(item)
                    # Explicitly attach rank to discovery record
                    n.add_discovery(keyword=kw, rank=idx, source_type="synthetic_http")
                    notes.append(n)
                    ref = NoteReference(
                        note_id=n.note_id,
                        driver_name=self.name,
                        title_hint=n.title,
                        author_hint=n.author_name,
                        access_token=f"http_token_{n.note_id}",
                        discovered_keyword=kw,
                        rank=idx,
                        can_resume_cross_process=True
                    )
                    references.append(ref)
            except Exception as e:
                errors.append(f"Keyword '{kw}' query error: {str(e)}")
                is_partial = True

        return CrawlResponse(
            success=len(notes) > 0 or len(errors) == 0,
            driver_name=self.name,
            total_notes=len(notes),
            notes=notes,
            references=references,
            errors=errors,
            is_partial=is_partial,
            is_synthetic=True,
            data_source="synthetic_simulation", actual_request_count=0
        )

    async def get_note_detail(self, target: Union[str, NoteReference]) -> Optional[UnifiedNote]:
        if not self.config.get("allow_synthetic", False):
            raise CapabilityUnsupportedError("HTTP demonstration detail requires explicit allow_synthetic consent")
        await self.health_check()
        note_id = target.note_id if isinstance(target, NoteReference) else target

        if isinstance(target, NoteReference) and target.is_expired():
            raise ReferenceExpiredError(f"HTTP reference for {note_id} has expired.", note_id=note_id)

        note = self._parse_raw_note({
            "note_id": note_id,
            "title": f"【HTTP驱动详情】笔记 {note_id}",
            "desc": "这是 HttpCrawlerDriver 离线生成的详情演示，不包含真实平台采集结果。",
            "liked_count": "1.2万",
            "nickname": "HTTP作者"
        })
        note.is_synthetic = True
        note.data_source = "synthetic_simulation"
        if isinstance(target, NoteReference) and target.discovered_keyword:
            note.add_discovery(keyword=target.discovered_keyword, rank=target.rank, source_type="reference")
        return note
