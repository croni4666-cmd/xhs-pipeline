# -*- coding: utf-8 -*-
"""
Mock Driver for xhs-pipeline (v0.3.0-aligned)
Synthetic data provider for offline testing and continuous integration without browser dependencies.
"""

import asyncio
from typing import Optional, List, Union
from .base import BaseCrawlerDriver
from ..contracts import (
    CrawlRequest,
    CrawlResponse,
    UnifiedNote,
    NoteReference,
    EngagementMetrics,
    ContentCompleteness,
    DriverCapabilities,
    ReferenceExpiredError
)


class MockDriver(BaseCrawlerDriver):
    """Synthetic test driver for offline development, contract verification, and CI."""

    @property
    def name(self) -> str:
        return "mock"

    @property
    def capabilities(self) -> DriverCapabilities:
        return DriverCapabilities(
            can_search=True,
            can_get_detail=True,
            can_get_comments=True,
            can_get_media=False,
            requires_browser=False,
            supports_resume=True
        )

    async def health_check(self) -> bool:
        return True

    async def crawl_keywords(self, request: CrawlRequest) -> CrawlResponse:
        await asyncio.sleep(0.01)
        notes: List[UnifiedNote] = []
        references: List[NoteReference] = []

        for kw in request.keywords:
            for i in range(1, min(request.max_count_per_keyword, 5) + 1):
                note_id = f"mock_{kw}_{i}"
                note = UnifiedNote(
                    note_id=note_id,
                    title=f"【测试样板】关于 {kw} 的深度评测 #{i}",
                    desc=f"这是一篇关于 {kw} 的测试长文。包含完整路线规划、花费与避坑细节。\n1. 第一天行程规划\n2. 推荐指数：5颗星\n#测试[话题]# #{kw}[话题]#",
                    completeness=ContentCompleteness.FULL,
                    note_type="normal",
                    author_id=f"author_hash_{i}",
                    author_name=f"测试博主_{i}",
                    metrics=EngagementMetrics.from_raw(
                        likes=f"{i * 1200}",
                        collects=f"{i * 850}",
                        comments=f"{i * 45}",
                        shares=f"{i * 12}"
                    ),
                    url=f"https://www.xiaohongshu.com/explore/{note_id}",
                    tag_list=["测试", kw, "热门打卡"],
                    image_list=["https://example.com/mock.jpg"]
                )
                note.add_discovery(keyword=kw, rank=i)
                notes.append(note)

                # Generate matching reference with driver context
                ref = NoteReference(
                    note_id=note_id,
                    driver_name=self.name,
                    title_hint=note.title,
                    author_hint=note.author_name,
                    access_token=f"mock_token_{note_id}",
                    access_context={"session_id": "mock_sess_123"},
                    discovered_keyword=kw,
                    rank=i,
                    can_resume_cross_process=True
                )
                references.append(ref)

        return CrawlResponse(
            success=True,
            driver_name=self.name,
            total_notes=len(notes),
            notes=notes,
            references=references
        )

    async def get_note_detail(self, target: Union[str, NoteReference]) -> Optional[UnifiedNote]:
        note_id = target.note_id if isinstance(target, NoteReference) else target

        if isinstance(target, NoteReference) and target.is_expired():
            raise ReferenceExpiredError(f"Access token for note {note_id} has expired.", note_id=note_id)

        return UnifiedNote(
            note_id=note_id,
            title="【测试详情】单篇笔记详情样板",
            desc="单篇笔记详细正文，无需虚构搜索关键词。",
            completeness=ContentCompleteness.FULL,
            note_type="normal",
            author_id="author_mock",
            author_name="测试作者",
            metrics=EngagementMetrics.from_raw(100, 50, 20, 5),
            url=f"https://www.xiaohongshu.com/explore/{note_id}"
        )
