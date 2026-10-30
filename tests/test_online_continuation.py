import asyncio
from dataclasses import asdict
import json

import pytest

from core import __version__
from core.contracts import (ContentInsight, CrawlRequest, CrawlResponse, CrawlTaskManifest,
                            DriverCapabilities, EngagementMetrics, FieldPresence, PipelineError,
                            StageStatus, UnifiedNote)
from core.control import RuntimeCapabilities, SessionState
from core.drivers.base import BaseCrawlerDriver
from core.pipeline import XhsPipeline
from test_snapshot_cli import run_cli


class IncrementalDriver(BaseCrawlerDriver):
    def __init__(self, name='inc_driver', pause_on=None):
        self._name = name
        self.pause_on = pause_on
        self.requested_keywords = []
        self.runtime = None

    @property
    def name(self):
        return self._name

    @property
    def capabilities(self):
        return DriverCapabilities()

    @property
    def runtime_capabilities(self):
        return RuntimeCapabilities(True, True, True)

    async def health_check(self):
        return True

    async def get_note_detail(self, target):
        return None

    async def prepare_run(self, runtime):
        self.runtime = runtime
        runtime.mark_ready()

    async def crawl_keywords(self, request: CrawlRequest) -> CrawlResponse:
        self.requested_keywords.append(list(request.keywords))
        notes = []
        for kw in request.keywords:
            await self.runtime.before_request()
            if self.pause_on == kw:
                try:
                    self.runtime.pause(SessionState.RATE_LIMITED)
                except PipelineError as exc:
                    if notes:
                        exc.partial_response = CrawlResponse(
                            False, self.name, len(notes), notes, is_partial=True, data_source='live'
                        )
                    raise
            for idx in range(request.max_count_per_keyword):
                note = UnifiedNote(
                    f'{kw}_{idx}', title=f'Title {kw} {idx}', desc=f'Full body for {kw} {idx}.',
                    metrics=EngagementMetrics.from_raw(likes='100', comments='5'),
                    media_presence=FieldPresence.KNOWN_EMPTY, data_source='live'
                )
                note.add_discovery(kw, rank=idx + 1, discovered_at=100 + idx)
                notes.append(note)
        return CrawlResponse(True, self.name, len(notes), notes, data_source='live', actual_request_count=len(request.keywords))


def make_pipeline(tmp_path):
    settings = tmp_path / 'settings.json'
    settings.write_text(json.dumps({
        'checkpoint_dir': str(tmp_path / 'state'),
        'fallback_driver': None,
        'require_runtime_control': True,
        'control_scope': 'continue-scope',
    }), encoding='utf-8')
    return XhsPipeline(str(settings)), settings


def test_online_continuation_skips_completed_keywords_and_merges_insights(tmp_path):
    pipe, _ = make_pipeline(tmp_path)
    note_a = UnifiedNote('kwA_0', title='Title kwA 0', desc='Full body for kwA 0.',
                         metrics=EngagementMetrics.from_raw(likes='50'),
                         media_presence=FieldPresence.KNOWN_EMPTY,
                         stage_status=StageStatus.ANALYZED, data_source='live')
    note_a.add_discovery('kwA', rank=1, discovered_at=10)
    note_a.add_insight(ContentInsight('ins_a', 'kwA_0', 'guide', ['theme'],
                                      evidence_snippets=['Full body for kwA 0.'],
                                      input_content_hash=note_a.content_hash, confidence_score=0.9))
    manifest = CrawlTaskManifest(
        'parent_task', 'inc_driver', 'v1', __version__, 10, completed_at=20,
        keywords=['kwA', 'kwB'], request_quota=2, actual_discovered_count=1, deduplicated_count=1,
        stop_reason='online_paused',
        discovery_trails=[{'note_id': 'kwA_0', **asdict(note_a.discovery_records[0])}],
        content_hashes={'kwA_0': note_a.content_hash},
        effective_config={'request': {'keywords': ['kwA', 'kwB'], 'limit_per_keyword': 1}},
        config_fingerprint='a' * 64, data_source='live'
    )
    partial_resp = CrawlResponse(False, 'inc_driver', 1, [note_a], is_partial=True,
                                 manifest=manifest, data_source='live')
    snap_path = tmp_path / 'paused.json'
    pipe.save_task_snapshot(partial_resp, str(snap_path))

    driver = IncrementalDriver('inc_driver')
    pipe.register_driver(driver)
    continued = asyncio.run(pipe.continue_task_snapshot(str(snap_path)))
    assert driver.requested_keywords == [['kwB']]
    assert continued.success and not continued.is_partial
    assert [n.note_id for n in continued.notes] == ['kwA_0', 'kwB_0']
    assert len(continued.notes[0].insights) == 1
    assert continued.manifest.operation == 'online_continue'
    assert continued.manifest.parent_task_id == 'parent_task'
    assert continued.manifest.keywords == ['kwA', 'kwB']
    assert continued.manifest.effective_config['completed_keywords'] == ['kwA']
    assert continued.manifest.effective_config['continued_keywords'] == ['kwB']
    assert len(continued.snapshot_lineage) == 1 and continued.snapshot_lineage[0].task_id == 'parent_task'

    out_snap = tmp_path / 'completed.json'
    pipe.save_task_snapshot(continued, str(out_snap))
    reloaded = pipe.load_task_snapshot(str(out_snap))
    assert reloaded.manifest.operation == 'online_continue'
    assert [n.note_id for n in reloaded.notes] == ['kwA_0', 'kwB_0']


def test_online_continuation_short_circuits_with_zero_requests_when_quota_met(tmp_path, monkeypatch):
    pipe, _ = make_pipeline(tmp_path)
    driver = IncrementalDriver('inc_driver')
    pipe.register_driver(driver)
    initial = asyncio.run(pipe.execute_crawl(CrawlRequest(['kwA', 'kwB'], max_count_per_keyword=1), 'inc_driver'))
    snap_path = tmp_path / 'done.json'
    pipe.save_task_snapshot(initial, str(snap_path))

    def forbidden(*_):
        raise AssertionError('Driver should not be invoked when all keywords met quota')
    monkeypatch.setattr(pipe, 'execute_crawl', forbidden)
    continued = asyncio.run(pipe.continue_task_snapshot(str(snap_path)))
    assert continued.success and not continued.is_partial
    assert continued.actual_request_count == 0
    assert continued.manifest.operation == 'online_continue'
    assert continued.manifest.parent_task_id == initial.manifest.task_id
    assert len(continued.notes) == 2


def test_unacknowledged_pause_blocks_continuation_and_preserves_prior_notes(tmp_path):
    pipe, _ = make_pipeline(tmp_path)
    driver = IncrementalDriver('inc_driver', pause_on='kwB')
    pipe.register_driver(driver)
    req = CrawlRequest(['kwA', 'kwB', 'kwC'], max_count_per_keyword=1)
    with pytest.raises(PipelineError) as exc_info:
        asyncio.run(pipe.execute_crawl(req, 'inc_driver'))
    first_partial = exc_info.value.partial_response
    assert [n.note_id for n in first_partial.notes] == ['kwA_0']
    snap1 = tmp_path / 'snap1.json'
    pipe.save_task_snapshot(first_partial, str(snap1))

    # Attempting to continue while scope is still paused must fail closed and retain kwA_0 + lineage
    with pytest.raises(PipelineError) as exc_info2:
        asyncio.run(pipe.continue_task_snapshot(str(snap1), driver_name='inc_driver'))
    preserved = exc_info2.value.partial_response
    assert [n.note_id for n in preserved.notes] == ['kwA_0']
    assert preserved.manifest.operation == 'online_continue'
    assert preserved.manifest.parent_task_id == req.task_id
    assert len(preserved.snapshot_lineage) == 1

    # After manual acknowledgment, continuation fetches only kwB and kwC
    runtime = pipe.create_runtime(CrawlRequest([]))
    pipe.control_store.acknowledge(runtime.scope)
    driver.pause_on = None
    driver.requested_keywords.clear()
    completed = asyncio.run(pipe.continue_task_snapshot(str(snap1), driver_name='inc_driver'))
    assert driver.requested_keywords == [['kwB', 'kwC']]
    assert [n.note_id for n in completed.notes] == ['kwA_0', 'kwB_0', 'kwC_0']


def test_cli_continue_from_snapshot_and_budget_stats_reporting(tmp_path):
    first = run_cli(tmp_path, [
        '--driver', 'mock', '--allow-synthetic', '--keywords', '甲,乙',
        '--limit', '1', '--snapshot-file', 'step1.json'
    ], 0)
    assert 'task_driver_attempts' in first['budget_stats']
    # Denied without --allow-synthetic on synthetic snapshot
    denied = run_cli(tmp_path, ['--continue-from-snapshot', 'step1.json'], 2)
    assert denied['success'] is False
    # Continuing with a higher limit (2 per keyword) fetches remaining notes and records online_continue
    continued = run_cli(tmp_path, [
        '--continue-from-snapshot', 'step1.json', '--driver', 'mock',
        '--allow-synthetic', '--limit', '2', '--snapshot-file', 'step2.json'
    ], 0)
    assert continued['success'] is True
    assert continued['operation'] == 'online_continue'
    assert continued['parent_task_id'] == first['task_id']
    assert continued['total_notes'] >= 2
