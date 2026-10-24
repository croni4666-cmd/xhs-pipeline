import asyncio
import json
import sys
import time

import pytest
from core.control import ControlStore, RuntimeContext, RequestPolicy
from core.contracts import CrawlRequest, PipelineError
from core.drivers.mediacrawler import MediaCrawlerDriver


def controlled(tmp_path):
    folder = tmp_path / 'mc'
    (folder / 'config').mkdir(parents=True)
    (folder / 'config' / 'base_config.py').write_text('KEYWORDS=""\n', encoding='utf-8')
    driver = MediaCrawlerDriver({'mediacrawler_path': str(folder)})
    runtime = RuntimeContext(ControlStore(tmp_path / 'state'), 'p', 's', 'task', RequestPolicy(per_minute=100))
    async def ready(): return True
    driver.health_check = ready
    return driver, runtime


def test_zero_exit_with_risk_logs_pauses_instead_of_succeeding(tmp_path, monkeypatch):
    driver, runtime = controlled(tmp_path)
    class Process:
        returncode = 0
        stdin = None
        def __init__(self):
            self.stdout, self.stderr = asyncio.StreamReader(), asyncio.StreamReader()
            self.stdout.feed_data(b'Access restricted: HTTP 429\n'); self.stdout.feed_eof(); self.stderr.feed_eof()
        async def wait(self): return 0
    async def spawn(*args, **kwargs): return Process()
    monkeypatch.setattr('core.drivers.mediacrawler.asyncio.create_subprocess_exec', spawn)
    async def run():
        await driver.prepare_run(runtime)
        with pytest.raises(PipelineError): await driver.crawl_keywords(CrawlRequest(['audit']))
    asyncio.run(run())
    assert runtime.report()['state'] == 'rate_limited' and runtime.report()['paused']


def test_risk_event_stops_a_running_local_child_promptly(tmp_path):
    from core.drivers.process_monitor import monitor_process
    async def run():
        program = "import time; print('RISK',flush=True); time.sleep(30)"
        process = await asyncio.create_subprocess_exec(sys.executable, '-c', program,
                   stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        driver, _ = controlled(tmp_path)
        def on_line(line, channel):
            if line == 'RISK': raise PipelineError('controlled stop')
        started = time.monotonic()
        with pytest.raises(PipelineError): await monitor_process(process, on_line, driver._terminate, 10)
        assert time.monotonic() - started < 5 and process.returncode is not None
    asyncio.run(run())


def test_missing_session_event_is_not_a_valid_session(tmp_path, monkeypatch):
    driver, runtime = controlled(tmp_path)
    class Process:
        returncode = 0
        stdin = None
        def __init__(self):
            self.stdout, self.stderr = asyncio.StreamReader(), asyncio.StreamReader()
            self.stdout.feed_eof(); self.stderr.feed_eof()
        async def wait(self): return 0
    async def spawn(*args, **kwargs): return Process()
    monkeypatch.setattr('core.drivers.mediacrawler.asyncio.create_subprocess_exec', spawn)
    async def run():
        await driver.prepare_run(runtime)
        with pytest.raises(PipelineError): await driver.crawl_keywords(CrawlRequest(['audit']))
    asyncio.run(run())


def test_malformed_event_and_oversized_output_fail_closed(tmp_path, monkeypatch):
    driver, runtime = controlled(tmp_path)
    class BadEventProcess:
        returncode = 0
        stdin = None
        def __init__(self):
            self.stdout, self.stderr = asyncio.StreamReader(), asyncio.StreamReader()
            self.stdout.feed_data(b'XHS_CONTROL_EVENT {"schema":"bad"}\n')
            self.stdout.feed_eof(); self.stderr.feed_eof()
        async def wait(self): return 0
    async def spawn_bad(*args, **kwargs): return BadEventProcess()
    monkeypatch.setattr('core.drivers.mediacrawler.asyncio.create_subprocess_exec', spawn_bad)
    async def run_bad():
        await driver.prepare_run(runtime)
        with pytest.raises(PipelineError, match='Malformed runtime control event'):
            await driver.crawl_keywords(CrawlRequest(['audit']))
    asyncio.run(run_bad())

    class OversizedProcess:
        returncode = 0
        stdin = None
        def __init__(self):
            self.stdout, self.stderr = asyncio.StreamReader(), asyncio.StreamReader()
            self.stdout.feed_data(b'x' * 70000 + b'\n')
            self.stdout.feed_eof(); self.stderr.feed_eof()
        async def wait(self): return 0
    async def spawn_oversized(*args, **kwargs): return OversizedProcess()
    monkeypatch.setattr('core.drivers.mediacrawler.asyncio.create_subprocess_exec', spawn_oversized)
    async def run_oversized():
        await driver.prepare_run(runtime)
        with pytest.raises(PipelineError, match='exceeds the supported bound'):
            await driver.crawl_keywords(CrawlRequest(['audit']))
    asyncio.run(run_oversized())


def test_guarded_subprocess_end_to_end_with_fake_provider(tmp_path):
    driver, runtime = controlled(tmp_path)
    mc = tmp_path / 'mc'
    (mc / 'data' / 'xhs' / 'jsonl').mkdir(parents=True, exist_ok=True)
    (mc / 'httpx.py').write_text(
        'class Response:\n'
        '    status_code = 200\n'
        '    def json(self): return {"ok": True}\n'
        'class AsyncClient:\n'
        '    async def request(self, method, url, **kwargs): return Response()\n',
        encoding='utf-8'
    )
    (mc / 'playwright').mkdir(exist_ok=True)
    (mc / 'playwright' / '__init__.py').write_text('', encoding='utf-8')
    (mc / 'playwright' / 'async_api.py').write_text(
        'class Page:\n'
        '    async def goto(self, url, **kwargs): return None\n'
        '    async def close(self): pass\n'
        'class BrowserContext:\n'
        '    async def new_page(self): return Page()\n'
        'class BrowserType:\n'
        '    async def launch(self, **kwargs): pass\n'
        '    async def launch_persistent_context(self, **kwargs): pass\n',
        encoding='utf-8'
    )
    (mc / 'media_platform' / 'xhs').mkdir(parents=True, exist_ok=True)
    (mc / 'media_platform' / '__init__.py').write_text('', encoding='utf-8')
    (mc / 'media_platform' / 'xhs' / '__init__.py').write_text('', encoding='utf-8')
    (mc / 'media_platform' / 'xhs' / 'client.py').write_text(
        'import httpx\n'
        'class XiaoHongShuClient:\n'
        '    async def query_self(self):\n'
        '        await httpx.AsyncClient().request("GET", "https://edith.xiaohongshu.com/api/sns/web/v1/user/selfinfo")\n'
        '        return {"data": {"result": {"success": True}}}\n'
        '    async def pong(self): return False\n'
        '    async def get_note_by_keyword(self, keyword):\n'
        '        await httpx.AsyncClient().request("POST", "https://edith.xiaohongshu.com/api/sns/web/v1/search/notes")\n'
        '        return {"items": [{"id": "n1"}, {"id": "n2"}]}\n',
        encoding='utf-8'
    )
    (mc / 'media_platform' / 'xhs' / 'login.py').write_text(
        'class XiaoHongShuLogin:\n'
        '    async def begin(self): pass\n',
        encoding='utf-8'
    )
    (mc / 'tools').mkdir(exist_ok=True)
    (mc / 'tools' / '__init__.py').write_text('', encoding='utf-8')
    (mc / 'tools' / 'cdp_browser.py').write_text(
        'class CDPBrowserManager:\n'
        '    async def cleanup(self, force=False): pass\n',
        encoding='utf-8'
    )
    (mc / 'main.py').write_text(
        'import asyncio, json\n'
        'from pathlib import Path\n'
        'from media_platform.xhs.client import XiaoHongShuClient\n'
        'async def run():\n'
        '    client = XiaoHongShuClient()\n'
        '    assert await client.pong()\n'
        '    res = await client.get_note_by_keyword("audit")\n'
        '    out = Path("data/xhs/jsonl/search_contents_2026.jsonl")\n'
        '    rows = [{"note_id": item["id"], "title": "T", "desc": "Body", "source_keyword": "audit"} for item in res["items"]]\n'
        '    out.write_text("\\n".join(json.dumps(r) for r in rows) + "\\n", encoding="utf-8")\n'
        'asyncio.run(run())\n',
        encoding='utf-8'
    )
    async def run():
        await driver.prepare_run(runtime)
        response = await driver.crawl_keywords(CrawlRequest(['audit'], max_count_per_keyword=1))
        assert response.success and len(response.notes) == 1
    asyncio.run(run())
    report = runtime.report()
    assert report['state'] == 'ready' and not report['paused'] and report['controlled_attempts'] == 2
