import asyncio
import json

import pytest

from core.contracts import (AuthenticationError, BudgetExceededError, CapabilityUnsupportedError,
                            CircuitBreakerOpenError, CrawlRequest, CrawlResponse, DriverCapabilities,
                            DriverError, RateLimitError, UnifiedNote)
from core.drivers.base import BaseCrawlerDriver
from core.pipeline import XhsPipeline
from core.resilience import ExponentialBackoff


class StubDriver(BaseCrawlerDriver):
    def __init__(self, name="primary", error=None, response=None):
        self._name, self.error, self.response = name, error, response
        self.calls = 0
    @property
    def name(self): return self._name
    @property
    def capabilities(self): return DriverCapabilities()
    async def health_check(self): return True
    async def get_note_detail(self, target): return None
    async def crawl_keywords(self, request):
        self.calls += 1
        if self.error: raise self.error
        return self.response or CrawlResponse(True, self.name, 0, [])


def pipeline(tmp_path, **settings):
    config = tmp_path / "settings.json"
    config.write_text(json.dumps({"checkpoint_dir": str(tmp_path / "state"), "fallback_driver": None,
                                  "rate_limit_per_minute": 1000, "retry_attempts": 1, **settings}), encoding="utf-8")
    return XhsPipeline(str(config))


@pytest.mark.parametrize("error,reason", [
    (AuthenticationError("offline"), "authentication_required"),
    (RateLimitError("offline"), "rate_limited"),
    (DriverError("offline"), "failed"),
    (asyncio.CancelledError(), "cancelled"),
])
def test_failed_and_cancelled_tasks_always_save_manifest(tmp_path, error, reason):
    pipe = pipeline(tmp_path)
    pipe.backoff = ExponentialBackoff(max_retries=1)
    driver = StubDriver(error=error)
    pipe.register_driver(driver)
    request = CrawlRequest(["audit"])
    with pytest.raises(type(error)):
        asyncio.run(pipe.execute_crawl(request, "primary"))
    manifest = pipe.checkpoint_store.load_task_manifest(request.task_id)
    assert manifest is not None and manifest.stop_reason == reason
    assert manifest.config_fingerprint and manifest.errors
    assert manifest.actual_request_count is None


def test_initial_and_fallback_budget_exhaustion_preserves_exception(tmp_path):
    async def run(limit):
        pipe = pipeline(tmp_path, max_request_budget=limit)
        pipe.register_driver(StubDriver(error=AuthenticationError("offline")))
        pipe.register_driver(StubDriver("backup"))
        request = CrawlRequest(["audit"])
        with pytest.raises(BudgetExceededError):
            await pipe.execute_crawl(request, "primary", "backup")
        manifest = pipe.checkpoint_store.load_task_manifest(request.task_id)
        assert manifest.stop_reason == "budget_exceeded"
    asyncio.run(run(0))
    asyncio.run(run(1))


def test_returned_failure_counts_for_circuit_breaker_across_tasks(tmp_path):
    pipe = pipeline(tmp_path)
    driver = StubDriver(response=CrawlResponse(False, "primary", 0, [], errors=["offline"]))
    pipe.register_driver(driver)
    async def run():
        for _ in range(3):
            response = await pipe.execute_crawl(CrawlRequest(["audit"]), "primary")
            assert response.success is False
        with pytest.raises(CircuitBreakerOpenError):
            await pipe.execute_crawl(CrawlRequest(["audit"]), "primary")
    asyncio.run(run())
    assert driver.calls == 3


def test_synthetic_plugin_response_cannot_bypass_pipeline_gate(tmp_path):
    pipe = pipeline(tmp_path)
    synthetic = UnifiedNote("id", desc="demo", is_synthetic=True, data_source="mock")
    driver = StubDriver(response=CrawlResponse(True, "primary", 1, [synthetic]))
    pipe.register_driver(driver)
    request = CrawlRequest(["audit"])
    with pytest.raises(CapabilityUnsupportedError):
        asyncio.run(pipe.execute_crawl(request, "primary"))
    assert pipe.checkpoint_store.load_task_manifest(request.task_id).stop_reason == "unsupported"


def test_manifest_records_effective_configuration_provenance_and_budget_units(tmp_path):
    pipe = pipeline(tmp_path)
    note = UnifiedNote("id", desc="body")
    note.add_discovery("audit", rank=3, discovered_at=123)
    pipe.register_driver(StubDriver(response=CrawlResponse(True, "primary", 1, [note], errors=["ordinary warning"])))
    response = asyncio.run(pipe.execute_crawl(CrawlRequest(["audit"], enable_media=True, sort_type="latest"), "primary"))
    manifest = response.manifest
    assert manifest.detail_success_count == 0 and manifest.detail_failed_count == 0
    assert manifest.filter_criteria["media"] is True
    assert manifest.config_fingerprint and manifest.effective_config
    assert manifest.actual_request_count is None
    assert manifest.budget_stats["budget_unit"] == "driver_attempt"
    assert len(manifest.driver_attempts) == 1


def test_retries_are_real_and_each_attempt_consumes_budget(tmp_path, monkeypatch):
    pipe = pipeline(tmp_path, retry_attempts=2)
    pipe.backoff = ExponentialBackoff(initial_delay=0, max_retries=2)
    driver = StubDriver(error=RateLimitError("offline"))
    pipe.register_driver(driver)
    async def no_sleep(_): pass
    monkeypatch.setattr("core.resilience.asyncio.sleep", no_sleep)
    request = CrawlRequest(["audit"])
    with pytest.raises(RateLimitError):
        asyncio.run(pipe.execute_crawl(request, "primary"))
    assert driver.calls == 2 and pipe.budget_manager.used_requests == 2
    assert len(pipe.checkpoint_store.load_task_manifest(request.task_id).driver_attempts) == 2


def test_failed_return_then_fallback_budget_exhaustion_is_not_masked(tmp_path):
    pipe = pipeline(tmp_path, max_request_budget=1)
    pipe.register_driver(StubDriver(response=CrawlResponse(False, "primary", 0, [], errors=["offline"])))
    pipe.register_driver(StubDriver("backup"))
    request = CrawlRequest(["audit"])
    with pytest.raises(BudgetExceededError):
        asyncio.run(pipe.execute_crawl(request, "primary", "backup"))
    assert pipe.checkpoint_store.load_task_manifest(request.task_id).stop_reason == "budget_exceeded"


def test_invalid_explicit_settings_are_not_silently_replaced(tmp_path):
    file = tmp_path / "bad.json"
    file.write_text("not valid json", encoding="utf-8")
    from core.contracts import PipelineError
    with pytest.raises(PipelineError):
        XhsPipeline(str(file))


def test_waiting_concurrent_attempts_cannot_overrun_budget(monkeypatch):
    from core.resilience import RequestBudgetManager
    clock = {"now": 0.0}
    monkeypatch.setattr("core.resilience.time.time", lambda: clock["now"])
    async def run():
        budget = RequestBudgetManager(max_budget=2, rate_limit_per_min=1)
        await budget.acquire()
        waiting, released = [], asyncio.Event()
        async def sleep(_):
            waiting.append(True)
            if len(waiting) == 2:
                clock["now"] = 61.0
                released.set()
            await released.wait()
        monkeypatch.setattr("core.resilience.asyncio.sleep", sleep)
        outcomes = await asyncio.wait_for(asyncio.gather(budget.acquire(), budget.acquire(), return_exceptions=True), 2)
        assert budget.used_requests == 2
        assert sum(isinstance(outcome, BudgetExceededError) for outcome in outcomes) == 1
    asyncio.run(run())


def test_packaged_defaults_match_the_source_example():
    from importlib.resources import files
    from pathlib import Path
    packaged = json.loads(files("core").joinpath("resources/settings.example.json").read_text(encoding="utf-8"))
    example = json.loads((Path(__file__).resolve().parents[1] / "config/settings.example.json").read_text(encoding="utf-8"))
    assert packaged == example
    assert packaged["fallback_driver"] is None and packaged["allow_synthetic"] is False
