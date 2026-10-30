import json
from pathlib import Path
import subprocess
import sys

from test_snapshot_cli import run_cli


ROOT = Path(__file__).resolve().parents[1]
ANALYZE = ROOT / 'scripts/analyze_insights.py'
EXPORT = ROOT / 'scripts/export_obsidian.py'


def run_script(script, cwd, args, expected):
    result = subprocess.run([sys.executable, str(script), *args, '--format', 'json'], cwd=cwd,
                            encoding='utf-8', capture_output=True, timeout=30)
    assert result.returncode == expected, (result.returncode, result.stdout, result.stderr)
    return json.loads(result.stdout)


def test_analyze_insights_supports_snapshot_synthetic_gate_and_clean_json_stdout(tmp_path):
    run_cli(tmp_path, [
        '--driver', 'mock', '--allow-synthetic', '--keywords', '避坑',
        '--limit', '1', '--snapshot-file', 'snap.json'
    ], 0)
    denied = run_script(ANALYZE, tmp_path, ['--snapshot', 'snap.json'], 1)
    assert denied['success'] is False
    out = run_script(ANALYZE, tmp_path, [
        '--input', 'snap.json', '--allow-synthetic', '--output-insights', 'insights.json'
    ], 0)
    assert out['success'] is True and out['total_notes'] == 1
    assert out['output_insights_path'] == 'insights.json'
    saved = json.loads((tmp_path / 'insights.json').read_text(encoding='utf-8'))
    assert len(saved) == 1 and saved[0]['content_hash']


def test_export_obsidian_supports_snapshot_and_jsonl_with_synthetic_gate(tmp_path):
    run_cli(tmp_path, [
        '--driver', 'mock', '--allow-synthetic', '--keywords', '演示',
        '--limit', '1', '--snapshot-file', 'snap.json'
    ], 0)
    denied = run_script(EXPORT, tmp_path, ['--snapshot', 'snap.json', '--output', 'cards_denied'], 1)
    assert denied['success'] is False and not (tmp_path / 'cards_denied').exists()
    exported = run_script(EXPORT, tmp_path, [
        '--input', 'snap.json', '--allow-synthetic', '--output', 'cards_ok'
    ], 0)
    assert exported['success'] is True and exported['obsidian_cards_count'] == 1
    assert len(list((tmp_path / 'cards_ok').glob('XHS_*.md'))) == 1
