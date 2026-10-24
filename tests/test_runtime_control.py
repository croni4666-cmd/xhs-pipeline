import asyncio
import json
from pathlib import Path

import pytest

from core.contracts import BudgetExceededError, PipelineError


def components():
    from core.control import ControlStore, RuntimeContext, RequestPolicy, SessionState
    return ControlStore, RuntimeContext, RequestPolicy, SessionState


def test_pause_survives_restart_and_different_driver(tmp_path):
    Store, Runtime, Policy, State = components()
    first = Runtime(Store(tmp_path), 'platform', 'same-profile', 'task_a', Policy())
    first.mark_ready()
    with pytest.raises(PipelineError): first.pause(State.CHALLENGE)
    second = Runtime(Store(tmp_path), 'platform', 'same-profile', 'task_b', Policy())
    with pytest.raises(PipelineError): asyncio.run(second.before_request(probe=True))
    assert second.report()['state'] == 'challenge'
    assert 'same-profile' not in ''.join(p.read_text() for p in tmp_path.glob('*.json'))


def test_acknowledge_requires_fresh_probe_and_keeps_request_history(tmp_path):
    Store, Runtime, Policy, State = components()
    store = Store(tmp_path)
    runtime = Runtime(store, 'platform', 'profile', 'task_a', Policy(per_minute=100))
    runtime.mark_ready()
    asyncio.run(runtime.before_request())
    with pytest.raises(PipelineError): runtime.pause(State.AUTH_REQUIRED)
    store.acknowledge(runtime.scope)
    again = Runtime(store, 'platform', 'profile', 'task_b', Policy(per_minute=100))
    with pytest.raises(PipelineError): asyncio.run(again.before_request())
    asyncio.run(again.before_request(probe=True))
    assert again.report()['state'] == 'unknown'
    assert again.report()['requests_24h'] == 2


def test_network_failure_does_not_request_authentication_or_pause(tmp_path):
    Store, Runtime, Policy, State = components()
    runtime = Runtime(Store(tmp_path), 'platform', 'profile', 'task', Policy())
    runtime.observe(State.NETWORK_ERROR)
    assert runtime.report()['state'] == 'network_error' and not runtime.report()['paused']


def test_concurrent_request_attempts_cannot_overrun_task_quota(tmp_path):
    Store, Runtime, Policy, State = components()
    runtime = Runtime(Store(tmp_path), 'platform', 'profile', 'task', Policy(max_per_task=2, per_minute=100))
    runtime.mark_ready()
    async def run():
        outcomes = await asyncio.gather(*(runtime.before_request() for _ in range(3)), return_exceptions=True)
        assert sum(isinstance(value, BudgetExceededError) for value in outcomes) == 1
    asyncio.run(run())
    assert runtime.report()['controlled_attempts'] == 2


def test_rolling_budget_survives_new_task_and_process_instance(tmp_path):
    Store, Runtime, Policy, State = components()
    one = Runtime(Store(tmp_path), 'p', 's', 'task_a', Policy(max_per_day=1, per_minute=100))
    one.mark_ready()
    asyncio.run(one.before_request())
    two = Runtime(Store(tmp_path), 'p', 's', 'task_b', Policy(max_per_day=1, per_minute=100))
    with pytest.raises(BudgetExceededError): asyncio.run(two.before_request(probe=True))


def test_corrupt_store_fails_closed_instead_of_resetting(tmp_path):
    Store, Runtime, Policy, State = components()
    runtime = Runtime(Store(tmp_path), 'p', 's', 'task', Policy())
    runtime.mark_ready()
    file = next(tmp_path.glob('*.json'))
    file.write_text('{broken', encoding='utf-8')
    with pytest.raises(PipelineError): asyncio.run(runtime.before_request(probe=True))
    assert file.read_text() == '{broken'


def test_scope_lease_serializes_tasks_until_cleanup(tmp_path):
    Store, Runtime, Policy, State = components()
    first = Runtime(Store(tmp_path), 'p', 's', 'task_a', Policy())
    second = Runtime(Store(tmp_path), 'p', 's', 'task_b', Policy())
    async def run():
        order = []
        async def another():
            async with second.lease(): order.append('second')
        async with first.lease():
            task = asyncio.create_task(another())
            await asyncio.sleep(.05)
            assert order == []
        await asyncio.wait_for(task, 2)
        assert order == ['second']
    asyncio.run(run())


def test_external_acknowledgment_invalidates_old_ready_context(tmp_path):
    Store, Runtime, Policy, State = components()
    store = Store(tmp_path)
    old = Runtime(store, 'p', 's', 'task', Policy())
    old.mark_ready()
    store.acknowledge(old.scope)
    with pytest.raises(PipelineError): asyncio.run(old.before_request())
