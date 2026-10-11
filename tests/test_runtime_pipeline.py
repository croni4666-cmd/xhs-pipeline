import asyncio
import json

import pytest
from core.contracts import CrawlRequest, CrawlResponse, DriverCapabilities, PipelineError
from core.drivers.base import BaseCrawlerDriver
from core.pipeline import XhsPipeline
from core.control import RuntimeCapabilities, SessionState


def pipeline(tmp_path):
    file = tmp_path / 'settings.json'
    file.write_text(json.dumps({'checkpoint_dir': str(tmp_path / 'state'), 'fallback_driver': None,
                               'require_runtime_control': True, 'control_scope': 'shared-profile'}), encoding='utf-8')
    return XhsPipeline(str(file))


class ControlledDriver(BaseCrawlerDriver):
    def __init__(self, name='one', blocked=False): self._name, self.blocked, self.calls = name, blocked, 0
    @property
    def name(self): return self._name
    @property
    def capabilities(self): return DriverCapabilities()
    @property
    def runtime_capabilities(self): return RuntimeCapabilities(True, True, True)
    async def health_check(self): return True
    async def get_note_detail(self, target): return None
    async def prepare_run(self, runtime): self.runtime = runtime; runtime.mark_ready()
    async def crawl_keywords(self, request):
        self.calls += 1
        await self.runtime.before_request()
        if self.blocked: self.runtime.pause(SessionState.CHALLENGE)
        return CrawlResponse(True, self.name, 0, [])


def test_risk_pause_prevents_retry_fallback_and_new_driver_after_restart(tmp_path):
    pipe = pipeline(tmp_path)
    first, backup = ControlledDriver(blocked=True), ControlledDriver('backup')
    pipe.register_driver(first); pipe.register_driver(backup)
    request = CrawlRequest(['audit'])
    with pytest.raises(PipelineError): asyncio.run(pipe.execute_crawl(request, 'one', 'backup'))
    assert first.calls == 1 and backup.calls == 0
    manifest = pipe.checkpoint_store.load_task_manifest(request.task_id)
    assert manifest.stop_reason == 'online_paused'
    assert manifest.budget_stats['runtime_control']['state'] == 'challenge'
    restarted = pipeline(tmp_path)
    another = ControlledDriver('different_framework')
    restarted.register_driver(another)
    with pytest.raises(PipelineError): asyncio.run(restarted.execute_crawl(CrawlRequest(['audit']), another.name))
    assert another.calls == 0


def test_controller_injected_without_touching_content_contracts(tmp_path):
    pipe = pipeline(tmp_path)
    driver = ControlledDriver()
    pipe.register_driver(driver)
    response = asyncio.run(pipe.execute_crawl(CrawlRequest(['audit']), 'one'))
    report = response.manifest.budget_stats['runtime_control']
    assert report['controlled_attempts'] == 1 and report['state'] == 'ready'
    assert response.manifest.actual_request_count is None


def test_unsupported_live_driver_is_refused_before_crawl(tmp_path):
    from test_pipeline_regressions import StubDriver
    pipe = pipeline(tmp_path)
    driver = StubDriver()
    pipe.register_driver(driver)
    with pytest.raises(PipelineError): asyncio.run(pipe.execute_crawl(CrawlRequest(['audit']), driver.name))
    assert driver.calls == 0


def test_simulation_does_not_consult_online_pause(tmp_path):
    pipe = pipeline(tmp_path)
    runtime = pipe.create_runtime(CrawlRequest(['audit']))
    with pytest.raises(PipelineError): runtime.pause(SessionState.AUTH_REQUIRED)
    response = asyncio.run(pipe.execute_crawl(CrawlRequest(['demo'], allow_synthetic=True), 'mock'))
    assert response.success and response.is_synthetic
