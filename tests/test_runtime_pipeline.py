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


def test_previous_ready_does_not_authorize_a_new_driver_without_a_probe(tmp_path):
    pipe = pipeline(tmp_path)
    old = ControlledDriver('old_framework')
    pipe.register_driver(old)
    assert asyncio.run(pipe.execute_crawl(CrawlRequest(['audit']), old.name)).success
    class SilentDriver(ControlledDriver):
        async def prepare_run(self, runtime): self.runtime = runtime
        async def crawl_keywords(self, request):
            self.calls += 1
            return CrawlResponse(True, self.name, 0, [])
    restarted = pipeline(tmp_path)
    unverified = SilentDriver('another_framework')
    restarted.register_driver(unverified)
    with pytest.raises(PipelineError, match='verify|verified'):
        asyncio.run(restarted.execute_crawl(CrawlRequest(['audit']), unverified.name))


@pytest.mark.parametrize('value', [None, 'true', 'false', 1, 0, [], {}])
def test_only_explicit_boolean_false_can_disable_runtime_control(tmp_path, value):
    from test_pipeline_regressions import StubDriver
    pipe = pipeline(tmp_path)
    pipe.settings['require_runtime_control'] = value
    driver = StubDriver()
    pipe.register_driver(driver)
    with pytest.raises(PipelineError):
        asyncio.run(pipe.execute_crawl(CrawlRequest(['audit']), driver.name))
    assert driver.calls == 0


def test_preparation_auth_failure_is_terminal_before_fallback(tmp_path):
    from core.contracts import AuthenticationError
    class Expired(ControlledDriver):
        async def prepare_run(self, runtime): raise AuthenticationError('session expired')
    pipe = pipeline(tmp_path)
    first, backup = Expired(), ControlledDriver('backup')
    pipe.register_driver(first); pipe.register_driver(backup)
    with pytest.raises(PipelineError):
        asyncio.run(pipe.execute_crawl(CrawlRequest(['audit']), first.name, backup.name))
    assert backup.calls == 0
    assert pipe.create_runtime(CrawlRequest([])).report()['paused']


def test_partial_notes_on_midrun_pause_are_recorded_in_manifest(tmp_path):
    from core.contracts import UnifiedNote
    class PartialPauseDriver(ControlledDriver):
        async def crawl_keywords(self, request):
            self.calls += 1
            await self.runtime.before_request()
            try:
                self.runtime.pause(SessionState.RATE_LIMITED)
            except PipelineError as exc:
                note = UnifiedNote('partial_1', title='T', desc='B', data_source='mediacrawler')
                exc.partial_response = CrawlResponse(False, self.name, 1, [note], is_partial=True, data_source='mediacrawler')
                raise
    pipe = pipeline(tmp_path)
    driver = PartialPauseDriver('partial_mc')
    pipe.register_driver(driver)
    request = CrawlRequest(['audit'])
    with pytest.raises(PipelineError) as exc_info:
        asyncio.run(pipe.execute_crawl(request, 'partial_mc'))
    manifest = pipe.checkpoint_store.load_task_manifest(request.task_id)
    assert manifest.stop_reason == 'online_paused'
    assert manifest.deduplicated_count == 1 and 'partial_1' in manifest.content_hashes
    assert exc_info.value.partial_response.manifest.task_id == request.task_id
