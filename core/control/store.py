"""Atomic per-scope pause and request ledger, shared across adapters/processes."""
import hashlib
import json
import math
from pathlib import Path
import re
import time
from ..contracts import BudgetExceededError, DriverError, PipelineError
from ..persistence import atomic_write_json, file_lock
from ..security import safe_component
from .contracts import ControlPausedError, PAUSE_STATES, SessionState


class ControlStore:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def scope_key(platform, alias):
        if not all(isinstance(value, str) and 0 < len(value) <= 512 for value in (platform, alias)):
            raise PipelineError('Control platform and scope must be nonempty strings')
        return hashlib.sha256(json.dumps([platform, alias], ensure_ascii=False).encode()).hexdigest()

    def path(self, scope):
        if not isinstance(scope, str) or not re.fullmatch('[0-9a-f]{64}', scope):
            raise PipelineError('Invalid control scope')
        return self.directory / ('scope_' + scope + '.json')

    def _load(self, path):
        if not path.exists():
            return {'schema': 'runtime_control_v1', 'state': 'unknown', 'paused': False,
                    'updated_at': time.time(), 'requests': []}
        try:
            if path.stat().st_size > 16 * 1024 * 1024:
                raise ValueError('Oversized ledger')
            def pairs(items):
                result = {}
                for key, value in items:
                    if key in result: raise ValueError('Duplicate ledger key')
                    result[key] = value
                return result
            with path.open(encoding='utf-8') as stream:
                data = json.load(stream, object_pairs_hook=pairs)
            if set(data) != {'schema', 'state', 'paused', 'updated_at', 'requests'} or data['schema'] != 'runtime_control_v1':
                raise ValueError('Invalid ledger schema')
            state = SessionState(data['state'])
            if type(data['paused']) is not bool or data['paused'] != (state in PAUSE_STATES):
                raise ValueError('Invalid ledger pause')
            if type(data['updated_at']) not in (float, int) or not math.isfinite(data['updated_at']):
                raise ValueError('Invalid timestamp')
            if not isinstance(data['requests'], list) or len(data['requests']) > 100000:
                raise ValueError('Invalid request history')
            for item in data['requests']:
                if not isinstance(item, dict) or set(item) != {'at', 'task'}:
                    raise ValueError('Invalid request record')
                safe_component(item['task'])
                if type(item['at']) not in (int, float) or not math.isfinite(item['at']):
                    raise ValueError('Invalid request time')
            return data
        except (ValueError, TypeError, OSError, KeyError, OverflowError, RecursionError):
            raise PipelineError('Control ledger is unreadable or invalid; online execution refused') from None

    def _transaction(self, scope, operation):
        path = self.path(scope)
        with file_lock(str(path) + '.lock'):
            data = self._load(path)
            result = operation(data)
            data['updated_at'] = time.time()
            atomic_write_json(path, data)
            return result

    def read(self, scope):
        path = self.path(scope)
        with file_lock(str(path) + '.lock'):
            return self._load(path)

    def observe(self, scope, state):
        state = SessionState(state)
        def update(data):
            if data['paused']: raise ControlPausedError(data['state'])
            data.update(state=state.value, paused=state in PAUSE_STATES)
        self._transaction(scope, update)

    def acknowledge(self, scope):
        def update(data): data.update(state='unknown', paused=False)
        self._transaction(scope, update)

    def reserve(self, scope, task_id, policy, *, probe=False):
        safe_component(task_id)
        def reserve(data):
            if data['paused']: raise ControlPausedError(data['state'])
            if probe is not True and data['state'] != 'ready':
                raise DriverError('Session has not been verified; normal request refused')
            now = time.time()
            # Future timestamps are retained conservatively when the clock moves backwards.
            data['requests'] = [item for item in data['requests'] if now - item['at'] < 86400]
            records = data['requests']
            used = sum(item['task'] == task_id for item in records)
            if used >= policy.max_per_task or len(records) >= policy.max_per_day:
                raise BudgetExceededError('Controlled request budget exhausted')
            recent = [item['at'] for item in records if now - item['at'] < 60]
            if len(recent) >= policy.per_minute:
                return max(.05, min(recent) + 60 - now)
            records.append({'at': now, 'task': task_id})
            return 0
        return self._transaction(scope, reserve)
