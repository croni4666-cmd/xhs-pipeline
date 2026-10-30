"""Offline acceptance of a previously installed wheel, outside its checkout."""
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import sysconfig
import tempfile


def main():
    entry = Path(sysconfig.get_path('scripts')) / ('xhs-pipeline.exe' if os.name == 'nt' else 'xhs-pipeline')
    env = dict(os.environ, PYTHONIOENCODING='utf-8')
    env.pop('PYTHONPATH', None)
    with tempfile.TemporaryDirectory(prefix='xhs-installed-') as temporary:
        cwd = Path(temporary)
        def run(args, expected):
            result = subprocess.run([str(entry), *args], cwd=cwd, env=env, text=True,
                                    encoding='utf-8', capture_output=True, timeout=30)
            assert result.returncode == expected, (result.returncode, result.stdout, result.stderr)
            return json.loads(result.stdout)
        denied = run(['--driver', 'mock', '--keywords', 'audit', '--format', 'json'], 2)
        assert 'error' in denied
        control = run(['--control-status', '--format', 'json'], 0)
        assert control['success'] and control['control']['state'] == 'unknown' and not control['control']['paused']
        acknowledged = run(['--acknowledge-pause', '--format', 'json'], 0)
        assert acknowledged['success'] and acknowledged['control']['state'] == 'unknown'
        run(['--driver', 'mock', '--allow-synthetic', '--keywords', 'audit', '--max-budget', '0', '--format', 'json'], 1)
        result = run(['--driver', 'mock', '--allow-synthetic', '--keywords', 'audit', '--limit', '2',
                      '--export-obsidian', 'cards', '--export-csv', 'notes.csv',
                      '--state-file', 'state.json', '--snapshot-file', 'task.json', '--analyze', '--format', 'json'], 0)
        assert result['success'] and result['total_notes'] == 2
        assert result['is_synthetic'] is True and result['data_sources'] == ['mock']
        assert result['budget_stats']['budget_unit'] == 'driver_attempt'
        state = json.loads((cwd / 'state.json').read_text(encoding='utf-8'))
        assert all(note['is_synthetic'] and note['data_source'] == 'mock' and note['media_presence'] for note in state)
        with (cwd / 'notes.csv').open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        assert len(rows) == 2 and all(row['media_presence'] for row in rows)
        assert len(list((cwd / 'data/checkpoints').glob('manifest_*.json'))) == 3
        run(['--resume-state', 'task.json', '--format', 'json'], 1)
        before = {card: (card.read_bytes(), card.stat().st_mtime_ns) for card in (cwd / 'cards').glob('*.md')}
        restored = run(['--resume-state', 'task.json', '--allow-synthetic', '--max-budget', '0',
                        '--export-obsidian', 'cards', '--snapshot-file', 'resumed.json', '--format', 'json'], 0)
        assert restored['operation'] == 'offline_resume' and restored['budget_stats']['used_requests'] == 0
        continued = run(['--continue-from-snapshot', 'task.json', '--allow-synthetic', '--max-budget', '0',
                         '--snapshot-file', 'continued.json', '--format', 'json'], 0)
        assert continued['operation'] == 'online_continue' and continued['budget_stats']['used_requests'] == 0
        assert all(card.read_bytes() == data and card.stat().st_mtime_ns == stamp for card, (data, stamp) in before.items())
        original = json.loads((cwd / 'task.json').read_text(encoding='utf-8'))['payload']
        resumed = json.loads((cwd / 'resumed.json').read_text(encoding='utf-8'))['payload']
        assert original['notes'] == resumed['notes']
        assert resumed['lineage'][0]['task_id'] == original['manifest']['task_id']
        assert resumed['manifest']['actual_request_count'] == 0
        subprocess.run([sys.executable, '-c', '''
from pathlib import Path
import core
from core.contracts import UnifiedNote
from core.pipeline import XhsPipeline
from core.storage import ObsidianExporter
assert 'site-packages' in str(Path(core.__file__)), core.__file__
pipe = XhsPipeline()
assert pipe.settings['fallback_driver'] is None
assert pipe.settings['allow_synthetic'] is False
assert pipe.settings['require_runtime_control'] is True
note = UnifiedNote('id', desc='body')
path = Path(pipe.export_obsidian_cards([note], 'idempotent', task_id='smoke')[0])
original = path.read_bytes()
path.write_bytes(original.replace(ObsidianExporter.USER_END.encode(), ('manual annotation\\n' + ObsidianExporter.USER_END).encode()))
before, stamp = path.read_bytes(), path.stat().st_mtime_ns
pipe.export_obsidian_cards([note], 'idempotent', task_id='smoke')
assert path.read_bytes() == before and path.stat().st_mtime_ns == stamp
note.desc = 'updated body'
pipe.export_obsidian_cards([note], 'idempotent', task_id='smoke')
assert b'manual annotation' in path.read_bytes()
assert list(path.parent.glob('*.bak'))
'''], cwd=cwd, env=env, check=True, timeout=30)
    print('Installed wheel smoke passed: defaults, gates, exits, manifest, CSV, snapshots, offline recovery, annotations and idempotence')


if __name__ == '__main__':
    main()
