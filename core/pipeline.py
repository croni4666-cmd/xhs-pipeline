# -*- coding: utf-8 -*-
"""
Pipeline Orchestrator (v0.3.0)
Coordinates multi-driver crawling, data normalization, failure recovery,
and non-destructive Obsidian markdown updates by delegating to dedicated components:
- DataCleaner (core/normalizer.py)
- ObsidianExporter & CsvExporter (core/storage.py)
- CheckpointStore (core/checkpoint.py)
- Resilience components (core/resilience.py)
"""

import os
import json
import time
from typing import List, Dict, Any, Optional
from pathlib import Path

from .contracts import (
    UnifiedNote,
    NoteReference,
    CrawlRequest,
    CrawlResponse,
    CrawlTaskManifest,
    EngagementMetrics,
    ContentCompleteness,
    FieldPresence,
    DiscoveryRecord,
    ContentInsight,
    StageStatus,
    PipelineError,
    RateLimitError,
    AuthenticationError,
    BudgetExceededError,
    CircuitBreakerOpenError
)
from .drivers.base import BaseCrawlerDriver
from .drivers.mediacrawler import MediaCrawlerDriver
from .drivers.mock_driver import MockDriver
from .drivers.http_driver import HttpCrawlerDriver
from .resilience import RequestBudgetManager, CircuitBreaker, DriverFallbackRouter, ExponentialBackoff
from .normalizer import DataCleaner
from .storage import ObsidianExporter, CsvExporter
from .checkpoint import CheckpointStore


class XhsPipeline:
    """Orchestrator coordinating drivers, normalization, resilience, and exporters."""

    def __init__(self, settings_path: Optional[str] = None):
        self.settings = self._load_settings(settings_path)
        self.drivers: Dict[str, BaseCrawlerDriver] = {}

        # 1. Resilience governance
        max_budget = int(self.settings.get("max_request_budget", 100))
        rate_limit = int(self.settings.get("rate_limit_per_minute", 30))
        self.budget_manager = RequestBudgetManager(max_budget=max_budget, rate_limit_per_min=rate_limit)
        self.backoff = ExponentialBackoff(initial_delay=1.0, factor=2.0, max_retries=3)

        # 2. Storage & checkpoint stores
        state_dir = self.settings.get("checkpoint_dir", "./data/checkpoints")
        self.checkpoint_store = CheckpointStore(state_dir=state_dir)
        self.cleaner = DataCleaner()

        self._register_default_drivers()

    def _load_settings(self, path: Optional[str]) -> Dict[str, Any]:
        default_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.json")
        target_path = path or default_path
        if os.path.exists(target_path):
            try:
                with open(target_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        example_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.example.json")
        if os.path.exists(example_path):
            try:
                with open(example_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "active_driver": "mediacrawler",
            "fallback_driver": "mock",
            "max_request_budget": 100,
            "rate_limit_per_minute": 30,
            "obsidian_vault_dir": os.environ.get("OBSIDIAN_VAULT_DIR", "./obsidian_cards"),
            "checkpoint_dir": "./data/checkpoints"
        }

    def _register_default_drivers(self):
        self.register_driver(MediaCrawlerDriver(self.settings))
        self.register_driver(MockDriver())
        self.register_driver(HttpCrawlerDriver(self.settings))

    def register_driver(self, driver: BaseCrawlerDriver):
        self.drivers[driver.name] = driver

    def get_driver(self, name: Optional[str] = None) -> BaseCrawlerDriver:
        driver_name = name or self.settings.get("active_driver", "mediacrawler")
        driver = self.drivers.get(driver_name)
        if not driver:
            raise PipelineError(f"Driver '{driver_name}' not found. Available: {list(self.drivers.keys())}")
        return driver

    async def execute_crawl(
        self,
        request: CrawlRequest,
        driver_name: Optional[str] = None,
        fallback_driver: Optional[str] = None
    ) -> CrawlResponse:
        """
        Executes search/crawl governed by budget limits, circuit breakers,
        and automatic driver failover. Saves complete task manifest for reproduction.
        """
        await self.budget_manager.acquire()

        selected_primary = driver_name or self.settings.get("active_driver", "mediacrawler")
        selected_fallback = fallback_driver or self.settings.get("fallback_driver")

        router = DriverFallbackRouter(primary_driver=selected_primary, fallback_driver=selected_fallback)
        active_name = router.select_healthy_driver()
        driver = self.get_driver(active_name)
        cb = router.get_circuit_breaker(active_name)

        started_at = time.time()
        response: Optional[CrawlResponse] = None

        try:
            response = await driver.crawl_keywords(request)
            cb.record_success()
        except (RateLimitError, AuthenticationError) as e:
            cb.record_failure()
            if selected_fallback and active_name != selected_fallback:
                fallback_driver_inst = self.get_driver(selected_fallback)
                fb_cb = router.get_circuit_breaker(selected_fallback)
                try:
                    response = await fallback_driver_inst.crawl_keywords(request)
                    fb_cb.record_success()
                    response.errors.append(f"Primary driver '{active_name}' failed ({str(e)}). Switched to '{selected_fallback}'.")
                except Exception as fb_err:
                    fb_cb.record_failure()
                    raise PipelineError(f"Both primary '{active_name}' and fallback '{selected_fallback}' failed: {fb_err}")
            else:
                raise
        except Exception:
            cb.record_failure()
            raise

        # Deduplicate notes across multi-keyword queries and merge discovery records
        raw_count = len(response.notes)
        deduped_notes = self.cleaner.deduplicate_notes(response.notes)
        response.notes = deduped_notes

        # Build research reproducibility manifest
        manifest = CrawlTaskManifest(
            task_id=request.task_id,
            driver_name=active_name,
            driver_version="0.3.0",
            pipeline_version="0.3.0",
            started_at=started_at,
            completed_at=time.time(),
            keywords=list(request.keywords),
            sort_type=request.sort_type,
            request_quota=request.max_count_per_keyword * len(request.keywords),
            actual_discovered_count=raw_count,
            deduplicated_count=len(deduped_notes),
            detail_success_count=len(deduped_notes),
            detail_failed_count=len(response.errors),
            stop_reason="completed" if response.success else "partial_failure",
            content_hashes={n.note_id: n.content_hash for n in deduped_notes}
        )
        response.manifest = manifest

        # Persist task manifest & references to checkpoint store
        self.checkpoint_store.save_task_manifest(manifest)
        if response.references:
            self.checkpoint_store.save_references(request.task_id, response.references)

        return response

    def load_from_jsonl(self, jsonl_path: str) -> List[UnifiedNote]:
        """
        Failure Recovery: Ingest raw JSONL cache and clean/deduplicate.
        """
        if not os.path.exists(jsonl_path):
            raise FileNotFoundError(f"JSONL cache file not found: {jsonl_path}")

        raw_notes: List[UnifiedNote] = []
        with open(jsonl_path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    tags = [t.strip() for t in raw.get("tag_list", "").split(",") if t.strip()]
                    images = [img.strip() for img in raw.get("image_list", "").split(",") if img.strip()]

                    metrics = EngagementMetrics.from_raw(
                        likes=raw.get("liked_count"),
                        collects=raw.get("collected_count"),
                        comments=raw.get("comment_count"),
                        shares=raw.get("share_count")
                    )

                    desc_text = str(raw.get("desc", ""))
                    note_type = str(raw.get("type", "normal"))
                    comp = self.cleaner.evaluate_completeness(note_type, desc_text)
                    raw_url = str(raw.get("note_url", ""))
                    clean_url = raw_url.split("?")[0] if "?" in raw_url else raw_url
                    note_id = str(raw.get("note_id", ""))

                    note = UnifiedNote(
                        note_id=note_id,
                        title=str(raw.get("title", "")),
                        desc=desc_text,
                        completeness=comp,
                        desc_presence=FieldPresence.VALID if desc_text else FieldPresence.KNOWN_EMPTY,
                        note_type=note_type,
                        author_id=str(raw.get("creator_hash", "")),
                        author_name=str(raw.get("nickname", "匿名用户")),
                        metrics=metrics,
                        url=clean_url or f"https://www.xiaohongshu.com/explore/{note_id}",
                        tag_list=tags,
                        image_list=images,
                        video_url=str(raw.get("video_url", "")),
                        published_at=raw.get("time"),
                        stage_status=StageStatus.CRAWLED
                    )
                    kw = str(raw.get("source_keyword", ""))
                    if kw:
                        note.add_discovery(keyword=kw)
                    raw_notes.append(note)
                except Exception:
                    continue

        return self.cleaner.deduplicate_notes(raw_notes)

    def export_csv(self, notes: List[UnifiedNote], output_path: str) -> str:
        """Delegates CSV table export to CsvExporter."""
        exporter = CsvExporter(output_path=output_path)
        return exporter.export(notes)

    def export_obsidian_cards(
        self,
        notes: List[UnifiedNote],
        vault_dir: Optional[str] = None,
        task_id: str = "default_task"
    ) -> List[str]:
        """
        Delegates atomic Obsidian card generation to ObsidianExporter
        and atomically commits state to CheckpointStore.
        """
        target_dir = vault_dir or self.settings.get("obsidian_vault_dir") or "./obsidian_export"
        exporter = ObsidianExporter(vault_dir=target_dir)
        written_files = []

        for note in notes:
            file_path, written_hash = exporter.export_note(note)
            written_files.append(file_path)
            # Atomic commit of export state to checkpoint store
            self.checkpoint_store.commit_export(task_id, note.note_id, file_path, written_hash)

        return written_files
