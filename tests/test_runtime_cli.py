import json
from core.control import ControlStore, RuntimeContext, SessionState
from core.contracts import PipelineError
import pytest
from test_snapshot_cli import run_cli


def test_local_status_and_acknowledgment_preserve_budget(tmp_path):
    store = ControlStore(tmp_path / 'state' / 'runtime_control')
    runtime = RuntimeContext(store, 'xhs', 'default', 'task')
    runtime.mark_ready()
    import asyncio
    asyncio.run(runtime.before_request())
    with pytest.raises(PipelineError): runtime.pause(SessionState.CHALLENGE)
    settings = tmp_path / 'settings.json'
    settings.write_text(json.dumps({'checkpoint_dir': str(tmp_path / 'state')}), encoding='utf-8')
    status = run_cli(tmp_path, ['--settings', str(settings), '--control-status'], 0)
    assert status['control']['state'] == 'challenge' and status['control']['paused']
    result = run_cli(tmp_path, ['--settings', str(settings), '--acknowledge-pause'], 0)
    assert result['control']['state'] == 'unknown' and not result['control']['paused']
    assert result['control']['requests_24h'] == 1
