import asyncio
from types import SimpleNamespace

import pytest
from core.control import ControlStore, RuntimeContext, RequestPolicy


def bindings():
    from core.drivers.mediacrawler_runtime import MediaCrawlerBindings
    class Client:
        async def query_self(self): return {'data': {'result': {'success': True}}}
        async def pong(self): return False
        async def get_note_by_keyword(self, keyword): return {'items': [{'id': '1'}, {'id': '2'}], 'has_more': True}
    class Login:
        called = False
        async def begin(self): self.called = True
    class Http:
        calls = 0
        response = SimpleNamespace(status_code=200, json=lambda: {'success': True})
        async def request(self, method, url, **kwargs): self.calls += 1; return self.response
    class Page:
        closed = False
        async def goto(self, url, **kwargs): return SimpleNamespace(status=200)
        async def close(self): self.closed = True
    class Context:
        closed = False
        async def new_page(self): return Page()
        async def close(self): self.closed = True
    class Browser:
        called = False
        async def launch(self, **kwargs): self.called = True
        async def launch_persistent_context(self, **kwargs): self.called = True
    class Cdp:
        async def cleanup(self, force=False): await self.browser_context.close()
    return MediaCrawlerBindings(Client, Login, Http, Page, Context, Browser, Cdp)


def runtime(tmp_path):
    return RuntimeContext(ControlStore(tmp_path), 'platform', 'scope', 'task', RequestPolicy(per_minute=100))


def test_network_probe_failure_does_not_call_login(tmp_path):
    from core.drivers.mediacrawler_runtime import install_guards, BridgeStop
    bundle = bindings(); ctx = runtime(tmp_path)
    async def failure(_): raise OSError('offline network')
    bundle.client.query_self = failure
    guard = install_guards(ctx, bundle, lambda _: None, 1)
    async def run():
        with pytest.raises(BridgeStop): await bundle.client().pong()
        assert not bundle.login.called
    try: asyncio.run(run())
    finally: guard.restore()
    assert ctx.report()['state'] == 'network_error' and not ctx.report()['paused']


def test_expired_session_and_direct_login_are_paused_without_login(tmp_path):
    from core.drivers.mediacrawler_runtime import install_guards, BridgeStop
    bundle = bindings(); ctx = runtime(tmp_path)
    guard = install_guards(ctx, bundle, lambda _: None, 1)
    async def run():
        with pytest.raises(BridgeStop): await bundle.login().begin()
        assert not bundle.login.called
    try: asyncio.run(run())
    finally: guard.restore()
    assert ctx.report()['state'] == 'auth_required' and ctx.report()['paused']


def test_http_restriction_stops_before_next_network_call(tmp_path):
    from core.drivers.mediacrawler_runtime import install_guards, BridgeStop
    bundle = bindings(); ctx = runtime(tmp_path)
    guard = install_guards(ctx, bundle, lambda _: None, 1)
    async def run():
        assert await bundle.client().pong()
        client = bundle.http()
        client.response = SimpleNamespace(status_code=429, json=lambda: {})
        with pytest.raises(BridgeStop): await client.request('GET', 'https://example.invalid/api')
        with pytest.raises(BridgeStop): await client.request('GET', 'https://example.invalid/again')
        assert client.calls == 1
    try: asyncio.run(run())
    finally: guard.restore()
    assert ctx.report()['paused'] and ctx.report()['controlled_attempts'] == 1


def test_cleanup_closes_only_owned_pages_and_browser_launch_is_refused(tmp_path):
    from core.drivers.mediacrawler_runtime import install_guards, BridgeStop
    bundle = bindings(); ctx = runtime(tmp_path)
    guard = install_guards(ctx, bundle, lambda _: None, 1)
    async def run():
        context = bundle.context()
        foreign = bundle.page()
        owned = await context.new_page()
        cdp = bundle.cdp(); cdp.browser_context = context
        await cdp.cleanup(force=True)
        assert owned.closed and not foreign.closed and not context.closed
        browser = bundle.browser()
        with pytest.raises(BridgeStop): await browser.launch()
        assert not browser.called
    try: asyncio.run(run())
    finally: guard.restore()


def test_actual_requested_note_count_is_respected_before_detail_fetch(tmp_path):
    from core.drivers.mediacrawler_runtime import install_guards
    bundle = bindings(); ctx = runtime(tmp_path)
    guard = install_guards(ctx, bundle, lambda _: None, 1)
    async def run():
        client = bundle.client()
        assert await client.pong()
        assert len((await client.get_note_by_keyword('keyword'))['items']) == 1
        assert (await client.get_note_by_keyword('keyword'))['items'] == []
    try: asyncio.run(run())
    finally: guard.restore()


def test_json_risk_code_and_browser_navigation_challenge_pause_session(tmp_path):
    from core.drivers.mediacrawler_runtime import install_guards, BridgeStop
    bundle = bindings(); ctx = runtime(tmp_path)
    guard = install_guards(ctx, bundle, lambda _: None, 1)
    async def run_api_code():
        assert await bundle.client().pong()
        client = bundle.http()
        client.response = SimpleNamespace(status_code=200, json=lambda: {'code': 300011})
        with pytest.raises(BridgeStop): await client.request('GET', 'https://example.invalid/api')
    try: asyncio.run(run_api_code())
    finally: guard.restore()
    assert ctx.report()['state'] == 'access_restricted' and ctx.report()['paused']

    bundle2 = bindings(); ctx2 = runtime(tmp_path / 'nav')
    async def challenged_goto(self, url, **kwargs): return SimpleNamespace(status=461)
    bundle2.page.goto = challenged_goto
    guard2 = install_guards(ctx2, bundle2, lambda _: None, 1)
    async def run_nav_challenge():
        assert await bundle2.client().pong()
        page = await bundle2.context().new_page()
        with pytest.raises(BridgeStop): await page.goto('https://example.invalid/note')
    try: asyncio.run(run_nav_challenge())
    finally: guard2.restore()
    assert ctx2.report()['state'] == 'challenge' and ctx2.report()['paused']
