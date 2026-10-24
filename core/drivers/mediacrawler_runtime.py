"""MediaCrawler-only bridge; policy and persistence remain backend-neutral."""
import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import runpy
import sys
from urllib.parse import urlsplit

from ..control import ControlPausedError, RuntimeContext, SessionState
from ..contracts import BudgetExceededError, DriverError


class BridgeStop(BaseException):
    """Terminal guard signal which provider retry/login handlers must not swallow."""


@dataclass
class MediaCrawlerBindings:
    client: type
    login: type
    http: type
    page: type
    context: type
    browser: type
    cdp: type


class MediaCrawlerGuards:
    def __init__(self, runtime, bindings, emit, note_limit):
        if type(note_limit) is not int or note_limit < 1:
            raise DriverError('Invalid guarded note limit')
        self.runtime, self.bindings, self.emit, self.note_limit = runtime, bindings, emit, note_limit
        self.originals, self.owned_pages, self.seen = [], [], {}
        self.probing = False

    def event(self, state):
        state = SessionState(state)
        try:
            self.runtime.observe(state)
        except ControlPausedError:
            self.emit({'schema': 'runtime_event_v1', 'state': state.value})
            raise BridgeStop(state.value)
        self.emit({'schema': 'runtime_event_v1', 'state': state.value})

    def pause(self, state):
        try:
            self.runtime.pause(state)
        except ControlPausedError:
            self.emit({'schema': 'runtime_event_v1', 'state': SessionState(state).value})
            raise BridgeStop(SessionState(state).value)

    async def permit(self, *, probe=False):
        try:
            await self.runtime.before_request(probe=probe)
        except ControlPausedError as exc:
            self.emit({'schema': 'runtime_event_v1', 'state': exc.state.value})
            raise BridgeStop(exc.state.value)
        except BudgetExceededError:
            self.emit({'schema': 'runtime_event_v1', 'stop': 'budget_exceeded'})
            raise BridgeStop('budget_exceeded')
        except DriverError:
            raise BridgeStop('session_unverified')

    def inspect_response(self, response, *, browser=False):
        status = getattr(response, 'status' if browser else 'status_code', None)
        if status == 401: self.pause(SessionState.AUTH_REQUIRED)
        if status == 429: self.pause(SessionState.RATE_LIMITED)
        if status in (461, 471): self.pause(SessionState.CHALLENGE)
        if status == 403: self.pause(SessionState.ACCESS_RESTRICTED)
        if not browser:
            try:
                data = response.json()
            except (ValueError, TypeError):
                return
            if isinstance(data, dict) and str(data.get('code')) in ('300011', '300012'):
                self.pause(SessionState.ACCESS_RESTRICTED)

    def patch(self, target, name, replacement):
        original = getattr(target, name, None)
        if not callable(original): raise DriverError('MediaCrawler runtime interface is unsupported')
        self.originals.append((target, name, original))
        setattr(target, name, replacement)
        return original

    async def cleanup_owned(self):
        for page in self.owned_pages:
            try: await page.close()
            except Exception: pass
        self.owned_pages.clear()

    def restore(self):
        for target, name, original in reversed(self.originals): setattr(target, name, original)
        self.originals.clear()

    def install(self):
        guard, b = self, self.bindings
        async def pong(client):
            guard.probing = True
            try:
                data = await client.query_self()
            except Exception:
                guard.event(SessionState.NETWORK_ERROR)
                raise BridgeStop('network_error')
            finally:
                guard.probing = False
            if isinstance(data, dict) and (
                data.get('data', {}).get('result', {}).get('success') is True
                or data.get('result', {}).get('success') is True
            ):
                guard.event(SessionState.READY)
                return True
            guard.pause(SessionState.AUTH_REQUIRED)

        async def login(_): guard.pause(SessionState.AUTH_REQUIRED)
        async def launch(*_, **__): raise BridgeStop('existing_browser_required')
        async def cleanup(_, force=False): await guard.cleanup_owned()

        async def request(client, method, url, **kwargs):
            local = urlsplit(str(url)).hostname in ('localhost', '127.0.0.1', '::1')
            if not local: await guard.permit(probe=guard.probing)
            try:
                response = await original_request(client, method, url, **kwargs)
            except Exception:
                if not local: guard.event(SessionState.NETWORK_ERROR)
                raise BridgeStop('network_error')
            if not local: guard.inspect_response(response)
            return response

        async def new_page(context, *args, **kwargs):
            page = await original_new_page(context, *args, **kwargs)
            guard.owned_pages.append(page)
            return page

        async def goto(page, url, **kwargs):
            local = urlsplit(str(url)).hostname in ('localhost', '127.0.0.1', '::1')
            if not local:
                await guard.permit(probe=page in guard.owned_pages and not guard.runtime.session_verified)
            try: response = await original_goto(page, url, **kwargs)
            except Exception:
                guard.event(SessionState.NETWORK_ERROR)
                raise BridgeStop('network_error')
            if response is not None and not local: guard.inspect_response(response, browser=True)
            return response

        async def search(client, *args, **kwargs):
            keyword = kwargs.get('keyword', args[0] if args else '')
            if guard.seen.get(keyword, 0) >= guard.note_limit:
                return {'items': [], 'has_more': False}
            result = await original_search(client, *args, **kwargs)
            if isinstance(result, dict) and isinstance(result.get('items'), list):
                remaining = max(0, guard.note_limit - guard.seen.get(keyword, 0))
                selected = [item for item in result['items'] if isinstance(item, dict) and item.get('model_type') not in ('rec_query', 'hot_query')][:remaining]
                guard.seen[keyword] = guard.seen.get(keyword, 0) + len(selected)
                return {
                    **result,
                    'items': selected,
                    'has_more': bool(result.get('has_more')) and guard.seen[keyword] < guard.note_limit,
                }
            return result

        try:
            self.patch(b.client, 'pong', pong)
            if not callable(getattr(b.client, 'query_self', None)): raise DriverError('Session probe is unsupported')
            self.patch(b.login, 'begin', login)
            self.patch(b.browser, 'launch', launch)
            self.patch(b.browser, 'launch_persistent_context', launch)
            self.patch(b.cdp, 'cleanup', cleanup)
            original_request = self.patch(b.http, 'request', request)
            original_new_page = self.patch(b.context, 'new_page', new_page)
            original_goto = self.patch(b.page, 'goto', goto)
            original_search = self.patch(b.client, 'get_note_by_keyword', search)
        except BaseException:
            self.restore()
            raise
        return self


def install_guards(runtime, bindings, emit, note_limit):
    return MediaCrawlerGuards(runtime, bindings, emit, note_limit).install()


def main():
    def emit(event): print('XHS_CONTROL_EVENT ' + json.dumps(event), flush=True)
    guard = None
    try:
        raw = sys.stdin.buffer.readline(16385)
        if len(raw) > 16384: raise DriverError('Runtime bridge configuration is oversized')
        config_data = json.loads(raw)
        if set(config_data) != {'runtime', 'note_limit'}: raise DriverError('Invalid adapter configuration')
        runtime = RuntimeContext.from_bridge_config(config_data['runtime'])
        runtime.check_pause()
        root = Path.cwd()
        if not (root / 'main.py').is_file(): raise DriverError('MediaCrawler entrypoint is missing')
        sys.path.insert(0, str(root))
        import httpx
        from playwright.async_api import Page, BrowserContext, BrowserType
        from media_platform.xhs.client import XiaoHongShuClient
        from media_platform.xhs.login import XiaoHongShuLogin
        from tools.cdp_browser import CDPBrowserManager
        bindings = MediaCrawlerBindings(XiaoHongShuClient, XiaoHongShuLogin, httpx.AsyncClient,
                                        Page, BrowserContext, BrowserType, CDPBrowserManager)
        guard = install_guards(runtime, bindings, emit, config_data['note_limit'])
        sys.argv = ['main.py']
        runpy.run_path(str(root / 'main.py'), run_name='__main__')
    except BridgeStop:
        raise SystemExit(2)
    except ControlPausedError as exc:
        emit({'schema': 'runtime_event_v1', 'state': exc.state.value})
        raise SystemExit(2)
    except Exception:
        print('Guarded MediaCrawler bridge is unavailable or incompatible', file=sys.stderr, flush=True)
        raise SystemExit(2)
    finally:
        if guard: guard.restore()


if __name__ == '__main__': main()
