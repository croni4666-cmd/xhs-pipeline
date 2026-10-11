from dataclasses import asdict
import hashlib
import json

import pytest

from core import __version__
from core.contracts import (ContentCompleteness, ContentInsight, CrawlResponse, CrawlTaskManifest,
                            EngagementMetrics, FieldPresence, NoteReference, PipelineError,
                            StageStatus, UnifiedNote)
from core.pipeline import XhsPipeline


@pytest.fixture
def snapshot_task(tmp_path):
    settings = tmp_path / 'settings.json'
    settings.write_text(json.dumps({'checkpoint_dir': str(tmp_path / 'checkpoints'), 'fallback_driver': None}), encoding='utf-8')
    pipe = XhsPipeline(str(settings))
    note = UnifiedNote('audit_1', title='完整样本', desc='可追溯正文。', author_id='platform_id', author_name='昵称',
                       metrics=EngagementMetrics.from_raw(likes='1.2万', comments='0'),
                       tag_list=['研究', '测试'], image_list=['https://example.invalid/image?width=320'],
                       media_presence=FieldPresence.VALID, published_at=100, crawled_at=200,
                       stage_status=StageStatus.ANALYZED, data_source='audit_fixture')
    note.add_discovery('甲', rank=2, discovered_at=123)
    note.add_discovery('乙', rank=4, discovered_at=124)
    note.insights = [ContentInsight('ins_1', note.note_id, 'guide', ['主题'], evidence_snippets=['可追溯正文。'],
                                   model_name='fixture', prompt_version='v2', input_content_hash=note.content_hash,
                                   confidence_score=.73, recommendations=['建议'], analyzed_at=250)]
    empty = UnifiedNote('audit_2', completeness=ContentCompleteness.UNKNOWN, desc_presence=FieldPresence.FETCH_FAILED,
                        media_presence=FieldPresence.UNSUPPORTED, crawled_at=201, data_source='audit_fixture')
    notes = [note, empty]
    manifest = CrawlTaskManifest('source_task', 'audit', 'driver_v1', __version__, 100, completed_at=200,
                                 keywords=['甲', '乙'], actual_discovered_count=3, deduplicated_count=2,
                                 detail_success_count=None, detail_failed_count=None, stop_reason='partial_success',
                                 content_hashes={n.note_id: n.content_hash for n in notes},
                                 discovery_trails=[{'note_id': note.note_id, **asdict(d)} for d in note.discovery_records],
                                 effective_config={'sort': 'latest'}, config_fingerprint='f' * 64,
                                 budget_stats={'budget_unit': 'driver_attempt', 'task_driver_attempts': 1})
    response = CrawlResponse(True, 'audit', 2, notes, manifest=manifest, is_partial=True,
                              errors=['partial fixture'], data_source='audit_fixture', actual_request_count=None,
                              detail_success_count=None, detail_failed_count=None)
    return pipe, response, tmp_path / 'snapshot.json'


def test_full_snapshot_roundtrip_preserves_facts_inferences_and_search_provenance(snapshot_task):
    pipe, response, path = snapshot_task
    pipe.save_task_snapshot(response, str(path))
    restored = pipe.load_task_snapshot(str(path))
    assert [asdict(n) for n in restored.notes] == [asdict(n) for n in response.notes]
    assert restored.manifest == response.manifest
    assert restored.success and restored.is_partial
    assert restored.actual_request_count is None and restored.detail_success_count is None
    assert restored.errors == ['partial fixture']
    assert restored.notes[0].get_render_input_hash() == response.notes[0].get_render_input_hash()
    assert len(restored.snapshot_hash) == 64


def test_snapshot_never_persists_access_context_or_private_output_paths(snapshot_task):
    pipe, response, path = snapshot_task
    response.references = [NoteReference('id', 'audit', access_token='FAKE_TOKEN', access_context={'headers': {'Cookie': 'FAKE_COOKIE'}})]
    response.output_files = {'cache': 'FAKE_PRIVATE_PATH'}
    response.manifest.effective_config = {'headers': {'Cookie': 'FAKE_CONFIG'}, 'endpoint': 'https://user:FAKE_PASS@example.invalid/?sig=FAKE_SIG'}
    response.manifest.driver_attempts = [{'driver': 'audit', 'error': 'stderr {"auth":"FAKE_AUTH"}'}]
    response.errors = ['token="FAKE_UNCLOSED']
    pipe.save_task_snapshot(response, str(path))
    text = path.read_text(encoding='utf-8')
    assert 'FAKE_' not in text and 'access_context' not in text and 'output_files' not in text
    restored = pipe.load_task_snapshot(str(path))
    assert restored.references == [] and restored.output_files == {}


def test_snapshot_sanitizes_mutated_urls_without_changing_in_memory_facts(snapshot_task):
    pipe, response, path = snapshot_task
    response.notes[0].image_list.append('https://user:FAKE_PASS@example.invalid/image?token=FAKE_TOKEN&width=1')
    original = list(response.notes[0].image_list)
    response.manifest.content_hashes = {n.note_id: n.content_hash for n in response.notes}
    pipe.save_task_snapshot(response, str(path))
    assert 'FAKE_' not in path.read_text(encoding='utf-8')
    assert response.notes[0].image_list == original
    assert pipe.load_task_snapshot(str(path)).notes[0].image_list[-1] == 'https://example.invalid/image?width=1'


def test_unapproved_synthetic_snapshot_is_rejected(snapshot_task):
    pipe, response, path = snapshot_task
    response.notes[0].is_synthetic = True
    response.notes[0].data_source = 'mock'
    pipe.save_task_snapshot(response, str(path))
    with pytest.raises(PipelineError):
        pipe.load_task_snapshot(str(path))
    assert pipe.load_task_snapshot(str(path), allow_synthetic=True).is_synthetic


def rewrite_snapshot(path, mutate):
    document = json.loads(path.read_text(encoding='utf-8'))
    mutate(document)
    document['payload_sha256'] = hashlib.sha256(json.dumps(document['payload'], ensure_ascii=False,
                                               sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    path.write_text(json.dumps(document, ensure_ascii=False), encoding='utf-8')


def test_corrupt_snapshot_fails_before_restoring_or_exporting(snapshot_task):
    pipe, response, path = snapshot_task
    pipe.save_task_snapshot(response, str(path))
    document = json.loads(path.read_text(encoding='utf-8'))
    document['payload']['notes'][0]['desc'] = 'changed'
    path.write_text(json.dumps(document), encoding='utf-8')
    with pytest.raises(PipelineError):
        pipe.load_task_snapshot(str(path))
    assert not list(path.parent.glob('XHS_*.md'))


@pytest.mark.parametrize('mutation', [
    lambda doc: doc.update(schema='future_v9'),
    lambda doc: doc['payload']['notes'][0].update(media_presence='invented'),
    lambda doc: doc['payload']['notes'][0].update(is_synthetic='false'),
    lambda doc: doc['payload']['notes'][0]['metrics'].update(likes=True),
    lambda doc: doc['payload']['notes'][0]['insights'][0].update(note_id='wrong_id'),
    lambda doc: doc['payload']['notes'][0].update(note_id='../escape'),
    lambda doc: doc['payload']['notes'].append(doc['payload']['notes'][0]),
    lambda doc: doc['payload']['response'].update(total_notes=0),
    lambda doc: doc['payload']['manifest']['content_hashes'].update(audit_1='0' * 64),
    lambda doc: doc['payload']['notes'][0].update(access_token='FAKE_INJECTED'),
])
def test_valid_checksum_cannot_bypass_schema_and_semantic_validation(snapshot_task, mutation):
    pipe, response, path = snapshot_task
    pipe.save_task_snapshot(response, str(path))
    rewrite_snapshot(path, mutation)
    with pytest.raises(PipelineError):
        pipe.load_task_snapshot(str(path), allow_synthetic=True)


def test_duplicate_keys_and_nonfinite_values_are_rejected(snapshot_task):
    pipe, _, path = snapshot_task
    for data in ['{"schema":"x","schema":"y","payload":{}}', '{"saved_at": NaN}', '[]', '{broken']:
        path.write_text(data, encoding='utf-8')
        with pytest.raises(PipelineError):
            pipe.load_task_snapshot(str(path))


def test_interrupted_snapshot_save_preserves_previous_valid_snapshot(snapshot_task, monkeypatch):
    pipe, response, path = snapshot_task
    pipe.save_task_snapshot(response, str(path))
    before = path.read_bytes()
    def failed_replace(*_): raise OSError('injected offline failure')
    monkeypatch.setattr('core.persistence.os.replace', failed_replace)
    with pytest.raises(PipelineError):
        pipe.save_task_snapshot(response, str(path))
    assert path.read_bytes() == before
    assert not list(path.parent.glob('*.tmp'))


def test_resume_creates_offline_manifest_without_touching_a_driver(snapshot_task, monkeypatch):
    pipe, response, path = snapshot_task
    pipe.save_task_snapshot(response, str(path))
    def forbidden(*_): raise AssertionError('Offline recovery touched a driver')
    monkeypatch.setattr(pipe, 'get_driver', forbidden)
    restored = pipe.resume_task_snapshot(str(path))
    assert restored.driver_name == 'offline_snapshot'
    assert restored.is_partial and restored.success
    assert restored.actual_request_count == 0
    assert restored.manifest.actual_request_count == 0
    assert restored.manifest.operation == 'offline_resume'
    assert restored.manifest.parent_task_id == 'source_task'
    assert restored.manifest.task_id != 'source_task'
    assert restored.manifest.source_snapshot_hash == restored.snapshot_hash
    assert restored.manifest.driver_attempts == []
    assert restored.manifest.budget_stats['task_driver_attempts'] == 0
    assert restored.snapshot_lineage == [response.manifest]
    assert pipe.checkpoint_store.load_task_manifest(restored.manifest.task_id) == restored.manifest


def test_two_resumptions_preserve_original_manifest_and_byte_idempotence(snapshot_task):
    from pathlib import Path
    from core.storage import ObsidianExporter
    pipe, response, path = snapshot_task
    pipe.save_task_snapshot(response, str(path))
    restored = pipe.resume_task_snapshot(str(path))
    vault = path.parent / 'cards'
    card = Path(pipe.export_obsidian_cards(restored.notes, str(vault), restored.manifest.task_id)[0])
    edited = card.read_bytes().replace(ObsidianExporter.USER_END.encode(), ('手工批注\n' + ObsidianExporter.USER_END).encode())
    card.write_bytes(edited)
    first_stamp = card.stat().st_mtime_ns
    next_path = path.parent / 'next.json'
    pipe.save_task_snapshot(restored, str(next_path))
    again = pipe.resume_task_snapshot(str(next_path))
    pipe.export_obsidian_cards(again.notes, str(vault), again.manifest.task_id)
    assert card.read_bytes() == edited and card.stat().st_mtime_ns == first_stamp
    assert [item.task_id for item in again.snapshot_lineage] == ['source_task', restored.manifest.task_id]
    assert again.snapshot_lineage[0].actual_discovered_count == 3
    assert again.notes[0].insights[0].evidence_snippets == ['可追溯正文。']


def test_resume_retains_failed_source_status(snapshot_task):
    pipe, response, path = snapshot_task
    response.success = False
    response.manifest.stop_reason = 'failed'
    pipe.save_task_snapshot(response, str(path))
    restored = pipe.resume_task_snapshot(str(path))
    assert not restored.success and restored.is_partial
    assert restored.snapshot_lineage[0].stop_reason == 'failed'
    assert restored.manifest.stop_reason == 'source_failed'


def test_snapshot_file_size_is_checked_before_overwriting(snapshot_task, monkeypatch):
    pipe, response, path = snapshot_task
    pipe.save_task_snapshot(response, str(path))
    before = path.read_bytes()
    from core import snapshot
    monkeypatch.setattr(snapshot, 'MAX_SNAPSHOT_BYTES', len(before) - 100)
    with pytest.raises(PipelineError):
        pipe.save_task_snapshot(response, str(path))
    assert path.read_bytes() == before
    with pytest.raises(PipelineError):
        pipe.load_task_snapshot(str(path))


@pytest.mark.parametrize('mutation', [
    lambda doc: doc['payload']['manifest'].update(operation='future_operation'),
    lambda doc: doc['payload']['manifest'].update(parent_task_id='missing_parent', operation='offline_resume', source_snapshot_hash='f' * 64),
    lambda doc: doc['payload']['notes'][0].update(crawled_at=10 ** 400),
])
def test_invalid_lineage_and_extreme_numbers_fail_as_snapshot_errors(snapshot_task, mutation):
    pipe, response, path = snapshot_task
    pipe.save_task_snapshot(response, str(path))
    rewrite_snapshot(path, mutation)
    with pytest.raises(PipelineError):
        pipe.load_task_snapshot(str(path))
