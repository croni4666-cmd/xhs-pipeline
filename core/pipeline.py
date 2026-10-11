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
import hashlib
from dataclasses import asdict
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
    CircuitBreakerOpenError,
    CapabilityUnsupportedError, DriverError
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
        self.backoff = ExponentialBackoff(initial_delay=1.0, factor=2.0, max_retries=int(self.settings.get("retry_attempts", 3)))

        # 2. Storage & checkpoint stores
        state_dir = self.settings.get("checkpoint_dir", "./data/checkpoints")
        self.checkpoint_store = CheckpointStore(state_dir=state_dir)
        self.cleaner = DataCleaner()
        self.router: Optional[DriverFallbackRouter] = None

        self._register_default_drivers()

    def _load_settings(self, path: Optional[str]) -> Dict[str, Any]:
        default_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.json")
        target_path = path or default_path
        if os.path.exists(target_path):
            try:
                with open(target_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                if path is not None:
                    raise PipelineError("Explicit settings file is invalid")
        elif path is not None:
            raise PipelineError("Explicit settings file does not exist")
        example_path = os.path.join(os.path.dirname(__file__), "..", "config", "settings.example.json")
        if os.path.exists(example_path):
            try:
                with open(example_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        from importlib.resources import files
        defaults = json.loads(files("core").joinpath("resources/settings.example.json").read_text(encoding="utf-8"))
        defaults["obsidian_vault_dir"] = os.environ.get("OBSIDIAN_VAULT_DIR", defaults["obsidian_vault_dir"])
        return defaults

    def _register_default_drivers(self):
        self.register_driver(MediaCrawlerDriver(self.settings))
        self.register_driver(MockDriver(self.settings))
        self.register_driver(HttpCrawlerDriver(self.settings))

    def register_driver(self, driver: BaseCrawlerDriver):
        self.drivers[driver.name] = driver

    def get_driver(self, name: Optional[str] = None) -> BaseCrawlerDriver:
        driver_name = name or self.settings.get("active_driver", "mediacrawler")
        driver = self.drivers.get(driver_name)
        if not driver:
            raise PipelineError(f"Driver '{driver_name}' not found. Available: {list(self.drivers.keys())}")
        return driver

    async def execute_crawl(self, request, driver_name=None, fallback_driver=None):
        import asyncio
        from . import __version__
        from .security import public_config, redact_error
        started = time.time()
        primary = driver_name or self.settings.get("active_driver", "mediacrawler")
        fallback = fallback_driver if fallback_driver is not None else self.settings.get("fallback_driver")
        fallback = fallback or None
        allowed = request.allow_synthetic is True or self.settings.get("allow_synthetic") is True
        attempts, diagnostics, used_drivers = [], [], {}
        response, active, stop_reason, raw_count = None, primary, "failed", 0
        request_snapshot = {"keywords": list(request.keywords), "limit_per_keyword": request.max_count_per_keyword,
                            "sort_type": request.sort_type, "comments": request.enable_comments,
                            "media": request.enable_media, "allow_synthetic": allowed}
        effective = {"request": request_snapshot, "settings": public_config(self.settings)}
        config_fingerprint = hashlib.sha256(json.dumps(effective, sort_keys=True, ensure_ascii=False).encode()).hexdigest()

        async def invoke(name):
            nonlocal active
            active = name
            driver = self.get_driver(name)
            used_drivers[name] = driver
            if getattr(driver, "is_synthetic", False) and not allowed:
                raise CapabilityUnsupportedError("Synthetic drivers require explicit allow_synthetic consent")
            breaker = self.router.get_circuit_breaker(name)
            backoff = getattr(self, "backoff", ExponentialBackoff(max_retries=1))

            async def attempt():
                if not breaker.can_execute():
                    raise CircuitBreakerOpenError("Driver circuit is open")
                await self.budget_manager.acquire()
                record = {"driver": name, "started_at": time.time(), "status": "pending", "actual_request_count": None}
                attempts.append(record)
                try:
                    result = await driver.crawl_keywords(request)
                    if not isinstance(result, CrawlResponse):
                        raise DriverError("Driver returned an invalid response")
                    if (result.is_synthetic or any(n.is_synthetic for n in result.notes)) and not allowed:
                        raise CapabilityUnsupportedError("Synthetic response rejected by pipeline")
                    result.errors = [redact_error(error) for error in result.errors]
                    count = result.actual_request_count
                    result.actual_request_count = count if type(count) is int and count >= 0 else None
                    record["actual_request_count"] = result.actual_request_count
                    record["status"] = "success" if result.success else "failed"
                    if result.success:
                        breaker.record_success()
                    else:
                        breaker.record_failure()
                    return result
                except BaseException as exc:
                    breaker.record_failure()
                    record["status"] = "cancelled" if isinstance(exc, asyncio.CancelledError) else "failed"
                    record["error"] = redact_error(str(exc) or type(exc).__name__)
                    raise
                finally:
                    record["completed_at"] = time.time()
            return await backoff.execute(attempt)

        try:
            if not getattr(self, "router", None):
                self.router = DriverFallbackRouter(primary, fallback)
            self.router.primary_driver, self.router.fallback_driver = primary, fallback
            active = self.router.select_healthy_driver()
            try:
                response = await invoke(active)
            except (AuthenticationError, RateLimitError, CircuitBreakerOpenError) as exc:
                if not fallback or active == fallback:
                    raise
                diagnostics.append(redact_error(f"Primary driver failed: {exc}"))
                response = await invoke(fallback)
            if response is not None and not response.success and fallback and active != fallback:
                diagnostics.extend(response.errors)
                response = await invoke(fallback)
            response.driver_name = active
            raw_count = len(response.notes)
            response.notes = self.cleaner.deduplicate_notes(response.notes)
            response.total_notes = len(response.notes)
            response.errors = diagnostics + response.errors
            response.is_synthetic = response.is_synthetic or any(n.is_synthetic for n in response.notes)
            stop_reason = "failed" if not response.success else "partial_success" if response.is_partial else "completed"
            return response
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                stop_reason = "cancelled"
            elif isinstance(exc, BudgetExceededError):
                stop_reason = "budget_exceeded"
            elif isinstance(exc, AuthenticationError):
                stop_reason = "authentication_required"
            elif isinstance(exc, RateLimitError):
                stop_reason = "rate_limited"
            elif isinstance(exc, CircuitBreakerOpenError):
                stop_reason = "circuit_open"
            elif isinstance(exc, CapabilityUnsupportedError):
                stop_reason = "unsupported"
            diagnostics.append(redact_error(str(exc) or type(exc).__name__))
            raise
        finally:
            for driver in used_drivers.values():
                try:
                    await driver.close()
                except Exception as exc:
                    diagnostics.append(redact_error(f"Driver cleanup failed: {exc}"))
                    if response and response.success:
                        response.is_partial = True
                        stop_reason = "partial_success"
            for name, driver in used_drivers.items():
                effective[name] = public_config(getattr(driver, "effective_config", getattr(driver, "config", getattr(driver, "cfg", {}))))
            config_fingerprint = hashlib.sha256(json.dumps(effective, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
            notes = response.notes if response else []
            trails = [{"note_id": n.note_id, **asdict(event)} for n in notes for event in n.discovery_records]
            reported = [a["actual_request_count"] for a in attempts]
            actual_count = sum(reported) if reported and all(type(v) is int and v >= 0 for v in reported) else None
            stats = {**self.budget_manager.get_stats(), "budget_unit": "driver_attempt",
                     "task_driver_attempts": len(attempts)}
            manifest = CrawlTaskManifest(
                task_id=request.task_id, driver_name=active,
                driver_version=getattr(used_drivers.get(active), "version", "unknown"),
                pipeline_version=__version__, started_at=started, completed_at=time.time(),
                keywords=list(request.keywords), sort_type=request.sort_type,
                request_quota=request.max_count_per_keyword * len(request.keywords),
                actual_discovered_count=raw_count if response else 0, deduplicated_count=len(notes),
                detail_success_count=response.detail_success_count if response else 0,
                detail_failed_count=response.detail_failed_count if response else 0,
                stop_reason=stop_reason, filter_criteria={"sort_type": request.sort_type, "comments": request.enable_comments, "media": request.enable_media},
                discovery_trails=trails, content_hashes={n.note_id: n.content_hash for n in notes},
                effective_config=effective, config_fingerprint=config_fingerprint,
                driver_attempts=attempts, budget_stats=stats, actual_request_count=actual_count,
                is_synthetic=bool(response and response.is_synthetic),
                data_source=response.data_source if response else "unknown",
                errors=[redact_error(error) for error in diagnostics + (response.errors if response else [])],
            )
            if response:
                response.manifest = manifest
            self.checkpoint_store.save_task_manifest(manifest)
            # Always replace the current task's references, even when this attempt found none.
            self.checkpoint_store.save_references(request.task_id, response.references if response else [])

    def load_from_jsonl(self, jsonl_path, allow_synthetic=False):
        from .normalizer import normalize_raw_note
        raw_notes = []
        with open(jsonl_path, encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    raw_notes.append(normalize_raw_note(raw, keyword=raw.get("source_keyword"),
                                                        data_source="jsonl_cache"))
                except (ValueError, TypeError, AttributeError) as exc:
                    raise PipelineError(f"Invalid JSONL record at line {line_number}") from exc
        if not (allow_synthetic is True or getattr(self, "settings", {}).get("allow_synthetic") is True):
            if any(n.is_synthetic or n.data_source in ("mock", "synthetic_simulation") for n in raw_notes):
                raise CapabilityUnsupportedError("Cached synthetic data requires explicit consent")
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
        Skips rewriting when cards on disk are verified unchanged (true idempotence).
        """
        target_dir = vault_dir or self.settings.get("obsidian_vault_dir") or "./obsidian_export"
        exporter = ObsidianExporter(vault_dir=target_dir)
        written_files = []

        reconciled = self.checkpoint_store.reconcile_after_crash(task_id, target_dir, notes)

        for note in notes:
            with exporter.card_lock(note.note_id):
                file_path, written_hash = exporter.export_note(note)
                written_files.append(file_path)
                render_hash = note.get_render_input_hash()
                previous = self.checkpoint_store.get_commit(task_id, note.note_id)
                current = {"file_path": file_path, "source_hash": note.content_hash,
                           "artifact_hash": written_hash, "render_input_hash": render_hash}
                if not previous or any(previous.get(key) != value for key, value in current.items()):
                    self.checkpoint_store.commit_export(task_id, note.note_id, **current)

        return written_files

    def save_state(self, notes: List[UnifiedNote], file_path: str) -> str:
        """Atomically saves pipeline execution state to JSON."""
        payload = [n.to_dict() for n in notes]
        self.checkpoint_store._atomic_write_json(str(file_path), payload)
        return file_path

    def save_task_snapshot(self, response: CrawlResponse, file_path: str) -> str:
        from .snapshot import save_snapshot
        return save_snapshot(response, file_path)

    def load_task_snapshot(self, file_path: str, *, allow_synthetic=False) -> CrawlResponse:
        from .snapshot import load_snapshot
        allowed = allow_synthetic is True or self.settings.get('allow_synthetic') is True
        return load_snapshot(file_path, allow_synthetic=allowed)

    def _record_offline_response(self, response, operation):
        """Record a downstream operation separately from the original capture."""
        from copy import deepcopy
        import uuid
        from . import __version__
        from .security import public_config, redact_error
        parent = response.manifest
        if parent is not None:
            response.snapshot_lineage.append(deepcopy(parent))
        started = time.time()
        response.driver_name = 'offline_snapshot' if operation == 'offline_resume' else 'offline_jsonl'
        response.references, response.output_files = [], {}
        response.actual_request_count = 0
        response.detail_success_count, response.detail_failed_count = 0, 0
        response.total_notes = len(response.notes)
        response.is_synthetic = response.is_synthetic or any(note.is_synthetic for note in response.notes)
        response.errors = [redact_error(error) for error in response.errors]
        effective = {'operation': operation, 'settings': public_config(self.settings),
                     'source_snapshot_hash': response.snapshot_hash, 'parent_task_id': parent.task_id if parent else None}
        manifest = CrawlTaskManifest(
            task_id='task_' + uuid.uuid4().hex, driver_name=response.driver_name, driver_version=__version__,
            pipeline_version=__version__, started_at=started, completed_at=time.time(),
            keywords=list(parent.keywords) if parent else [], sort_type=parent.sort_type if parent else 'unknown',
            request_quota=0, actual_discovered_count=0, deduplicated_count=len(response.notes),
            detail_success_count=0, detail_failed_count=0,
            stop_reason='source_failed' if not response.success else 'partial_success' if response.is_partial else 'completed',
            filter_criteria=deepcopy(parent.filter_criteria) if parent else {},
            discovery_trails=[{'note_id': note.note_id, **asdict(event)} for note in response.notes for event in note.discovery_records],
            content_hashes={note.note_id: note.content_hash for note in response.notes}, effective_config=effective,
            config_fingerprint=hashlib.sha256(json.dumps(effective, sort_keys=True, ensure_ascii=False).encode()).hexdigest(),
            budget_stats={**self.budget_manager.get_stats(), 'task_driver_attempts': 0}, actual_request_count=0,
            data_source=response.data_source, is_synthetic=response.is_synthetic, errors=list(response.errors),
            operation=operation, parent_task_id=parent.task_id if parent else None,
            source_snapshot_hash=response.snapshot_hash,
        )
        self.checkpoint_store.save_task_manifest(manifest)
        self.checkpoint_store.save_references(manifest.task_id, [])
        response.manifest = manifest
        return response

    def resume_task_snapshot(self, file_path: str, *, allow_synthetic=False) -> CrawlResponse:
        """Restore facts and inferences offline; retain original status and capture lineage."""
        response = self.load_task_snapshot(file_path, allow_synthetic=allow_synthetic)
        return self._record_offline_response(response, 'offline_resume')

    def import_jsonl_task(self, file_path: str, *, allow_synthetic=False) -> CrawlResponse:
        notes = self.load_from_jsonl(file_path, allow_synthetic=allow_synthetic)
        response = CrawlResponse(True, 'offline_jsonl', len(notes), notes, data_source='jsonl_cache',
                                 is_synthetic=any(note.is_synthetic for note in notes), actual_request_count=0)
        return self._record_offline_response(response, 'jsonl_import')
