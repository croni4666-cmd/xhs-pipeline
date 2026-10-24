"""Runtime policy injection shared by in-process drivers and subprocess bridges."""
import asyncio
from dataclasses import asdict
import time
from ..contracts import DriverError
from ..persistence import async_file_lock
from ..security import safe_component
from .contracts import ControlPausedError, PAUSE_STATES, RequestPolicy, SessionState
from .store import ControlStore


class RuntimeContext:
    def __init__(self, store, platform, alias, task_id, policy=None):
        self.store = store
        self.scope = store.scope_key(platform, alias)
        self.task_id = safe_component(task_id)
        self.policy = policy or RequestPolicy()
        self._verified = False

    def lease(self):
        return async_file_lock(str(self.store.path(self.scope)) + '.running.lock', timeout=3600)

    def check_pause(self):
        data = self.store.read(self.scope)
        if data['paused']: raise ControlPausedError(data['state'])

    def mark_ready(self):
        self.store.observe(self.scope, SessionState.READY)
        self._verified = True

    @property
    def session_verified(self):
        return self._verified and self.store.read(self.scope)['state'] == 'ready'

    def observe(self, state):
        state = SessionState(state)
        self._verified = state == SessionState.READY
        self.store.observe(self.scope, state)

    def pause(self, state):
        state = SessionState(state)
        if state not in PAUSE_STATES: raise ValueError('This state is not a risk pause')
        self._verified = False
        self.store.observe(self.scope, state)
        raise ControlPausedError(state)

    async def before_request(self, *, probe=False):
        self.check_pause()
        if not self.session_verified and probe is not True:
            raise DriverError('Session has not been verified; normal request refused')
        while True:
            delay = self.store.reserve(self.scope, self.task_id, self.policy)
            if not delay: return
            await asyncio.sleep(min(delay, 1))

    def report(self):
        data = self.store.read(self.scope)
        records = [item for item in data['requests'] if time.time() - item['at'] < 86400]
        return {'scope_hash': self.scope, 'state': data['state'], 'paused': data['paused'],
                'controlled_attempts': sum(item['task'] == self.task_id for item in records),
                'requests_24h': len(records), 'coverage': 'adapter_hooks', 'limits': asdict(self.policy)}

    def bridge_config(self):
        return {'schema': 'runtime_bridge_v1', 'directory': str(self.store.directory),
                'scope': self.scope, 'task_id': self.task_id, 'policy': asdict(self.policy)}

    @classmethod
    def from_bridge_config(cls, data):
        if not isinstance(data, dict) or set(data) != {'schema', 'directory', 'scope', 'task_id', 'policy'} or data['schema'] != 'runtime_bridge_v1':
            raise DriverError('Invalid runtime bridge configuration')
        instance = cls.__new__(cls)
        instance.store = ControlStore(data['directory'])
        instance.scope = data['scope']
        instance.store.path(instance.scope)
        instance.task_id = safe_component(data['task_id'])
        instance.policy = RequestPolicy(**data['policy'])
        instance._verified = False
        return instance
