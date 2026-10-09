# -*- coding: utf-8 -*-
"""
Base Crawler Driver Interface (v0.3.0)
Defines the strict behavioral and lifecycle contract for all Xiaohongshu scraper drivers.

Guarantees:
- Clean decoupling of NoteReference (access context) from UnifiedNote (business fact).
- Explicit capability declaration via DriverCapabilities.
- Standardized lifecycle (initialize, close, health_check).
- Proper cancellation and resource cleanup semantics.
- Non-blocking asynchronous design.
"""

from abc import ABC, abstractmethod
from typing import Optional, List, Union
from ..contracts import (
    CrawlRequest,
    CrawlResponse,
    UnifiedNote,
    NoteReference,
    DriverCapabilities,
    CapabilityUnsupportedError,
    ReferenceExpiredError,
    DriverError
)


class BaseCrawlerDriver(ABC):
    """Abstract base class establishing the contract for scraper implementations."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier of the driver (e.g. 'mediacrawler', 'http', 'mock')."""
        pass

    @property
    @abstractmethod
    def capabilities(self) -> DriverCapabilities:
        """Explicit capability declaration indicating supported operational features."""
        pass

    async def initialize(self) -> None:
        """
        Lifecycle hook: Allocate resources, start background sessions, or verify connections.
        Default implementation is a no-op.
        """
        pass

    async def close(self) -> None:
        """
        Lifecycle hook: Release network connections, browser processes, or file handles.
        Default implementation is a no-op.
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """
        Verify driver readiness and operational status.
        Returns:
            bool: True if driver and underlying environment/session are ready.
        """
        pass

    @abstractmethod
    async def crawl_keywords(self, request: CrawlRequest) -> CrawlResponse:
        """
        Execute search crawl for requested keywords.
        Returns normalized UnifiedNote instances and internal references.
        """
        pass

    async def search_references(self, request: CrawlRequest) -> List[NoteReference]:
        """
        Phase 1 discovery: Returns lightweight query references containing
        driver-specific access tokens and session context.
        """
        res = await self.crawl_keywords(request)
        return res.references

    @abstractmethod
    async def get_note_detail(self, target: Union[str, NoteReference]) -> Optional[UnifiedNote]:
        """
        Fetch details for a specific note using a stable note_id or NoteReference.
        If a token has expired or is invalid, raises ReferenceExpiredError.
        """
        pass
