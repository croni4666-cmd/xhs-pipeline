import asyncio
import json
import os
import time
from pathlib import Path

from core.contracts import CrawlRequest
from core.drivers.mediacrawler import MediaCrawlerDriver


def fixture(tmp_path):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "base_config.py").write_text('KEYWORDS="old"\n', encoding="utf-8")
    data = tmp_path / "data" / "xhs" / "jsonl"
    data.mkdir(parents=True)
    driver = MediaCrawlerDriver({"mediacrawler_path": str(tmp_path), "require_runtime_control": False})
    async def ready(): return True
    driver.health_check = ready
    return driver, data


def test_recent_unchanged_cache_is_not_new_task_output(tmp_path, monkeypatch):
    driver, data = fixture(tmp_path)
    file = data / "search_contents_recent.jsonl"
    file.write_text(json.dumps({"note_id": "OLD", "desc": "old", "source_keyword": "audit"}) + "\n", encoding="utf-8")
    class Process:
        returncode = 0
        async def communicate(self): return b"", b""
    async def spawn(*args, **kwargs): return Process()
    monkeypatch.setattr("core.drivers.mediacrawler.asyncio.create_subprocess_exec", spawn)
    response = asyncio.run(driver.crawl_keywords(CrawlRequest(["audit"])))
    assert not response.notes


def test_appended_cache_reads_only_new_records(tmp_path, monkeypatch):
    driver, data = fixture(tmp_path)
    file = data / "search_contents_recent.jsonl"
    file.write_text(json.dumps({"note_id": "OLD", "desc": "old", "source_keyword": "audit"}) + "\n", encoding="utf-8")
    class Process:
        returncode = 0
        async def communicate(self):
            with file.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps({"note_id": "NEW", "desc": None, "image_list": None, "source_keyword": "audit"}) + "\n")
            return b"", b""
    async def spawn(*args, **kwargs): return Process()
    monkeypatch.setattr("core.drivers.mediacrawler.asyncio.create_subprocess_exec", spawn)
    response = asyncio.run(driver.crawl_keywords(CrawlRequest(["audit"])))
    assert [n.note_id for n in response.notes] == ["NEW"]
    assert response.notes[0].desc_presence.value == "not_fetched"


def test_cancelled_running_child_is_terminated_and_awaited(tmp_path, monkeypatch):
    driver, _ = fixture(tmp_path)
    class Process:
        returncode = None
        terminated = False
        waited = 0
        async def communicate(self): raise asyncio.CancelledError()
        def terminate(self): self.terminated = True
        def kill(self): self.returncode = -9
        async def wait(self):
            self.waited += 1
            self.returncode = -15
            return self.returncode
    process = Process()
    async def spawn(*args, **kwargs): return process
    monkeypatch.setattr("core.drivers.mediacrawler.asyncio.create_subprocess_exec", spawn)
    async def run():
        try:
            await driver.crawl_keywords(CrawlRequest(["audit"]))
        except asyncio.CancelledError:
            pass
        else:
            assert False, "Cancellation must propagate"
    asyncio.run(run())
    assert process.terminated and process.waited and driver._current_subprocess is None
