import json
from pathlib import Path
import subprocess
import sys

import pytest
from test_task_snapshots import snapshot_task


CLI = Path(__file__).resolve().parents[1] / 'scripts/run_pipeline.py'


def run_cli(cwd, args, expected):
    result = subprocess.run([sys.executable, str(CLI), *args, '--format', 'json'], cwd=cwd,
                            encoding='utf-8', capture_output=True, timeout=30)
    assert result.returncode == expected, (result.returncode, result.stdout, result.stderr)
    return json.loads(result.stdout)


def test_cli_live_snapshot_then_offline_recovery_keeps_insights_and_requires_consent(tmp_path):
    run_cli(tmp_path, ['--driver', 'mock', '--allow-synthetic', '--keywords', 'demo', '--limit', '1', '--analyze',
                       '--snapshot-file', 'task.json', '--state-file', 'flat.json', '--export-obsidian', 'cards'], 0)
    assert isinstance(json.loads((tmp_path / 'flat.json').read_text(encoding='utf-8')), list)
    document = json.loads((tmp_path / 'task.json').read_text(encoding='utf-8'))
    source_id = document['payload']['manifest']['task_id']
    original_insights = document['payload']['notes'][0]['insights']
    assert len(original_insights) == 1
    assert document['payload']['notes'][0]['stage_status'] == 'analyzed'
    denied = run_cli(tmp_path, ['--resume-state', 'task.json', '--export-obsidian', 'denied'], 1)
    assert denied['success'] is False and not (tmp_path / 'denied').exists()
    restored = run_cli(tmp_path, ['--resume-state', 'task.json', '--allow-synthetic', '--snapshot-file', 'restored.json',
                                  '--export-obsidian', 'cards', '--max-budget', '0'], 0)
    assert restored['operation'] == 'offline_resume'
    assert restored['parent_task_id'] == source_id
    assert restored['budget_stats']['used_requests'] == 0
    assert restored['is_synthetic'] and restored['data_sources'] == ['mock']
    next_document = json.loads((tmp_path / 'restored.json').read_text(encoding='utf-8'))
    assert next_document['payload']['notes'][0]['insights'] == original_insights
    assert next_document['payload']['manifest']['actual_request_count'] == 0


@pytest.mark.parametrize('success,partial,exit_code', [(True, True, 1), (False, True, 2)])
def test_cli_recovery_does_not_claim_incomplete_capture_succeeded(snapshot_task, success, partial, exit_code):
    pipe, response, path = snapshot_task
    response.success, response.is_partial = success, partial
    response.manifest.stop_reason = 'partial_success' if success else 'failed'
    pipe.save_task_snapshot(response, str(path))
    result = run_cli(path.parent, ['--resume-state', str(path), '--export-csv', 'recovered.csv'], exit_code)
    assert result['success'] == success and result['is_partial'] == partial
    assert (path.parent / 'recovered.csv').exists()


def test_cli_rejects_multiple_input_modes_and_damaged_snapshots_before_export(tmp_path):
    result = subprocess.run([sys.executable, str(CLI), '--keywords', 'demo', '--resume-state', 'no-file.json'],
                            cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 2
    (tmp_path / 'broken.json').write_text('{bad', encoding='utf-8')
    output = run_cli(tmp_path, ['--resume-state', 'broken.json', '--export-obsidian', 'cards'], 1)
    assert output['success'] is False and not (tmp_path / 'cards').exists()


def test_raw_jsonl_import_can_create_a_complete_offline_snapshot(tmp_path):
    (tmp_path / 'raw.jsonl').write_text('{"note_id":"id","title":"title","desc":"body"}\n', encoding='utf-8')
    imported = run_cli(tmp_path, ['--resume-from', 'raw.jsonl', '--snapshot-file', 'imported.json'], 0)
    assert imported['operation'] == 'jsonl_import'
    result = run_cli(tmp_path, ['--resume-state', 'imported.json', '--export-csv', 'notes.csv'], 0)
    assert result['total_notes'] == 1


def test_budget_failure_keeps_a_recoverable_failure_snapshot(tmp_path):
    result = run_cli(tmp_path, ['--driver', 'mock', '--allow-synthetic', '--keywords', 'audit',
                              '--max-budget', '0', '--snapshot-file', 'failed.json'], 1)
    assert result['error'] == 'BudgetExceededError'
    document = json.loads((tmp_path / 'failed.json').read_text(encoding='utf-8'))
    assert document['payload']['response']['success'] is False
    assert document['payload']['manifest']['stop_reason'] == 'budget_exceeded'
    resumed = run_cli(tmp_path, ['--resume-state', 'failed.json'], 2)
    assert resumed['success'] is False and resumed['total_notes'] == 0


def test_export_failure_leaves_snapshot_for_later_retry(tmp_path):
    (tmp_path / 'raw.jsonl').write_text('{"note_id":"id","desc":"body"}\n', encoding='utf-8')
    cards = tmp_path / 'cards'
    cards.mkdir()
    original = '---\nnote_id: "another_id"\n---\nmanual notes\n'
    (cards / 'XHS_id.md').write_text(original, encoding='utf-8')
    result = run_cli(tmp_path, ['--resume-from', 'raw.jsonl', '--snapshot-file', 'retry.json', '--export-obsidian', 'cards'], 1)
    assert result['success'] is False
    assert (cards / 'XHS_id.md').read_text(encoding='utf-8') == original
    recovered = run_cli(tmp_path, ['--resume-state', 'retry.json', '--export-obsidian', 'new-cards'], 0)
    assert recovered['obsidian_cards_count'] == 1
