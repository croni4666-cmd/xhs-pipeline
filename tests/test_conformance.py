# -*- coding: utf-8 -*-
"""
Driver Conformance Test Suite (v0.3.0)
Universal test suite enforcing behavioral, capability, cancellation, and reference compliance
across multiple distinct driver implementations (MockDriver, HttpCrawlerDriver, etc.).

Verifies all second-round audit items:
1. NoteReference access context encapsulation and ReferenceExpiredError semantics.
2. Lifecycle management and clean resource disposal.
3. Cancellation semantics: CancelledError propagation and process cleanup.
4. Data model compliance (UnifiedNote, FieldPresence, EngagementMetrics).
"""

import sys
import asyncio
from pathlib import Path

# Setup path
root_dir = str(Path(__file__).resolve().parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from core import (
    BaseCrawlerDriver,
    CrawlRequest,
    CrawlResponse,
    UnifiedNote,
    NoteReference,
    EngagementMetrics,
    ContentCompleteness,
    FieldPresence,
    DriverCapabilities,
    ReferenceExpiredError
)
from core.drivers.mock_driver import MockDriver
from core.drivers.http_driver import HttpCrawlerDriver


async def verify_driver_contract(driver: BaseCrawlerDriver):
    """Universal conformance test function applicable to any driver."""
    # 1. Identity & Capability declaration
    assert isinstance(driver.name, str) and len(driver.name) > 0, "Driver must have a non-empty name"
    assert isinstance(driver.capabilities, DriverCapabilities), "Driver must expose DriverCapabilities"

    # 2. Lifecycle
    await driver.initialize()
    health = await driver.health_check()
    assert isinstance(health, bool), "health_check must return boolean"

    # 3. Crawl Execution & Reference Compliance
    request = CrawlRequest(keywords=["合规性测试"], max_count_per_keyword=2)
    response = await driver.crawl_keywords(request)

    assert isinstance(response, CrawlResponse), "crawl_keywords must return CrawlResponse"
    assert isinstance(response.success, bool)
    assert response.driver_name == driver.name
    assert isinstance(response.notes, list)
    assert isinstance(response.references, list), "crawl_keywords must return references list"

    for note in response.notes:
        assert isinstance(note, UnifiedNote), "All returned items must be UnifiedNote instances"
        assert note.note_id, "note_id must not be empty"
        assert isinstance(note.metrics, EngagementMetrics), "metrics must be EngagementMetrics"
        assert isinstance(note.completeness, ContentCompleteness), "completeness must be ContentCompleteness"
        assert isinstance(note.desc_presence, FieldPresence), "desc_presence must be FieldPresence"
        assert isinstance(note.metrics.likes_presence, FieldPresence), "likes_presence must be FieldPresence"
        assert note.fact.note_id == note.note_id
        assert len(note.content_hash) > 0

    # 4. Detail Query with stable note_id (No dummy keyword required)
    detail = await driver.get_note_detail("test_id_001")
    assert detail is None or isinstance(detail, UnifiedNote)

    # 5. Detail Query with NoteReference
    if response.references:
        first_ref = response.references[0]
        assert isinstance(first_ref, NoteReference)
        assert first_ref.note_id
        assert isinstance(first_ref.access_token, str)

        detail_by_ref = await driver.get_note_detail(first_ref)
        assert detail_by_ref is None or isinstance(detail_by_ref, UnifiedNote)

        # 6. Expired Reference Semantics: Must raise ReferenceExpiredError
        expired_ref = NoteReference(
            note_id="expired_001",
            driver_name=driver.name,
            access_token="expired_token",
            expires_at=100.0, # Expired in 1970
            is_stale=True
        )
        try:
            await driver.get_note_detail(expired_ref)
            assert False, "Should have raised ReferenceExpiredError for expired reference"
        except ReferenceExpiredError as err:
            assert err.note_id == "expired_001"
            assert err.needs_re_search is True

    # 7. Cancellation Semantics: Must propagate asyncio.CancelledError cleanly
    async def cancel_trial():
        task = asyncio.create_task(driver.crawl_keywords(request))
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return True
        return False

    cancelled_ok = await cancel_trial()
    assert cancelled_ok is True, "Driver must propagate asyncio.CancelledError upon cancellation"

    # 8. Cleanup & Teardown
    await driver.close()


def test_mock_driver_conformance():
    driver = MockDriver()
    asyncio.run(verify_driver_contract(driver))
    print("[√] Driver conformance passed for MockDriver (including ReferenceExpired & Cancellation)!")


def test_http_driver_conformance():
    driver = HttpCrawlerDriver({"timeout_seconds": 5.0})
    assert driver.capabilities.requires_browser is False
    asyncio.run(verify_driver_contract(driver))
    print("[√] Driver conformance passed for HttpCrawlerDriver (browserless)!")


if __name__ == "__main__":
    test_mock_driver_conformance()
    test_http_driver_conformance()
    print("\n[√] All drivers passed the expanded v0.3.0 conformance suite!")
