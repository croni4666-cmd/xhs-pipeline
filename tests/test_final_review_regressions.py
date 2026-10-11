import asyncio
import csv
import io
import time

import pytest

from core.contracts import CrawlRequest, FieldPresence, RateLimitError, UnifiedNote
from core.normalizer import normalize_raw_note
from core.security import redact_error
from core.storage import CsvExporter
from test_pipeline_regressions import StubDriver, pipeline


@pytest.mark.parametrize('raw', [
    'stderr {"signature":"FAKE_SIG"}',
    'stderr {"auth":"FAKE_AUTH"}',
    'token="FAKE_UNCLOSED',
    'failed https://user:FAKE_PASS@example.invalid/a?sig=FAKE_SIG&width=320',
])
def test_prefixed_and_malformed_errors_do_not_expose_credentials(raw):
    assert 'FAKE_' not in redact_error(raw)


def test_unclosed_quoted_error_is_processed_without_backtracking():
    started = time.monotonic()
    result = redact_error('token="' + '\\' * 32)
    assert time.monotonic() - started < 0.2
    assert '[REDACTED]' in result


def test_retry_circuit_open_uses_healthy_fallback(tmp_path, monkeypatch):
    pipe = pipeline(tmp_path, retry_attempts=4)
    primary, backup = StubDriver(error=RateLimitError('offline')), StubDriver('backup')
    pipe.register_driver(primary)
    pipe.register_driver(backup)
    async def no_sleep(_): pass
    monkeypatch.setattr('core.resilience.asyncio.sleep', no_sleep)
    response = asyncio.run(pipe.execute_crawl(CrawlRequest(['audit']), 'primary', 'backup'))
    assert response.success and response.driver_name == 'backup'
    assert primary.calls == 3 and backup.calls == 1


@pytest.mark.parametrize('presence', list(FieldPresence))
def test_media_presence_survives_csv_and_state_reload(tmp_path, presence):
    note = UnifiedNote('id', media_presence=presence)
    data = note.to_dict()
    assert data['media_presence'] == presence.value
    assert normalize_raw_note(data).media_presence == presence
    path = CsvExporter(str(tmp_path / 'notes.csv')).export([note])
    row = next(csv.DictReader(io.StringIO(open(path, encoding='utf-8-sig').read())))
    assert row['media_presence'] == presence.value


def test_flat_snapshot_preserves_metrics_and_presence():
    from core.contracts import EngagementMetrics
    metrics = EngagementMetrics.from_raw(likes='1.2万', comments='0')
    metrics.shares_presence = FieldPresence.FETCH_FAILED
    note = UnifiedNote('id', metrics=metrics)
    restored = normalize_raw_note(note.to_dict())
    assert restored.metrics == metrics


@pytest.mark.parametrize('field', ['note_type', 'url'])
def test_untrusted_metadata_cannot_insert_annotation_markers(tmp_path, field):
    from core.storage import ObsidianExporter
    note = UnifiedNote('id', desc='body')
    setattr(note, field, 'https://example.invalid/<!-- BEGIN_USER_NOTES -->')
    exporter = ObsidianExporter(str(tmp_path))
    first = exporter.export_note(note)
    assert exporter.export_note(note) == first


def test_concurrent_manifests_count_only_their_own_attempts(tmp_path):
    from core.contracts import CrawlResponse
    pipe = pipeline(tmp_path)
    async def run():
        ready = asyncio.Event()
        class Waiting(StubDriver):
            async def crawl_keywords(self, request):
                self.calls += 1
                if self.calls == 2: ready.set()
                await ready.wait()
                return CrawlResponse(True, self.name, 0, [])
        pipe.register_driver(Waiting())
        responses = await asyncio.gather(*(pipe.execute_crawl(CrawlRequest(['audit']), 'primary') for _ in range(2)))
        assert all(response.manifest.budget_stats['task_driver_attempts'] == 1 for response in responses)
    asyncio.run(run())


@pytest.mark.parametrize('reported', [True, -1, 'unknown'])
def test_invalid_driver_request_count_remains_unknown(tmp_path, reported):
    from core.contracts import CrawlResponse
    pipe = pipeline(tmp_path)
    pipe.register_driver(StubDriver(response=CrawlResponse(True, 'primary', 0, [], actual_request_count=reported)))
    response = asyncio.run(pipe.execute_crawl(CrawlRequest(['audit']), 'primary'))
    assert response.manifest.actual_request_count is None
