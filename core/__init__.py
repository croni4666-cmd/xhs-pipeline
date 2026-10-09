# -*- coding: utf-8 -*-
"""
xhs-pipeline: Industrial Xiaohongshu Intelligence, Research Reproducibility & PKM Pipeline
"""

__version__ = "0.3.1rc2"
__author__ = "Antigravity Engineering"
__license__ = "MIT"

from .contracts import (
    UnifiedNote,
    UnifiedComment,
    ContentFact,
    EngagementSnapshot,
    EngagementMetrics,
    DiscoveryRecord,
    NoteReference,
    CrawlTaskManifest,
    FieldPresence,
    ContentCompleteness,
    DriverCapabilities,
    ContentInsight,
    StageStatus,
    CrawlRequest,
    CrawlResponse,
    PipelineError,
    DriverError,
    AuthenticationError,
    RateLimitError,
    ReferenceExpiredError,
    BudgetExceededError,
    CircuitBreakerOpenError
)
from .pipeline import XhsPipeline
from .normalizer import DataCleaner
from .storage import ObsidianExporter, CsvExporter
from .checkpoint import CheckpointStore
from .drivers.base import BaseCrawlerDriver
from .drivers.mediacrawler import MediaCrawlerDriver
from .drivers.mock_driver import MockDriver
from .drivers.http_driver import HttpCrawlerDriver
from .resilience import RequestBudgetManager, CircuitBreaker, DriverFallbackRouter, ExponentialBackoff

__all__ = [
    "__version__",
    "UnifiedNote",
    "UnifiedComment",
    "ContentFact",
    "EngagementSnapshot",
    "EngagementMetrics",
    "DiscoveryRecord",
    "NoteReference",
    "CrawlTaskManifest",
    "FieldPresence",
    "ContentCompleteness",
    "DriverCapabilities",
    "ContentInsight",
    "StageStatus",
    "CrawlRequest",
    "CrawlResponse",
    "PipelineError",
    "DriverError",
    "AuthenticationError",
    "RateLimitError",
    "ReferenceExpiredError",
    "BudgetExceededError",
    "CircuitBreakerOpenError",
    "XhsPipeline",
    "DataCleaner",
    "ObsidianExporter",
    "CsvExporter",
    "CheckpointStore",
    "BaseCrawlerDriver",
    "MediaCrawlerDriver",
    "MockDriver",
    "HttpCrawlerDriver",
    "RequestBudgetManager",
    "CircuitBreaker",
    "DriverFallbackRouter",
    "ExponentialBackoff",
]
