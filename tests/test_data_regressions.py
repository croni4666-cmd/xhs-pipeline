import ast
import asyncio
import json

import pytest

from core.contracts import CrawlRequest, CapabilityUnsupportedError, FieldPresence, ContentCompleteness, UnifiedNote
from core.drivers.http_driver import HttpCrawlerDriver
from core.drivers.mock_driver import MockDriver
from core.drivers.mediacrawler import MediaCrawlerDriver
from core.normalizer import DataCleaner
from core.pipeline import XhsPipeline


@pytest.mark.parametrize("driver", [HttpCrawlerDriver, MockDriver])
def test_simulation_requires_explicit_consent_for_search_and_detail(driver):
    instance = driver()
    async def run():
        with pytest.raises(CapabilityUnsupportedError):
            await instance.crawl_keywords(CrawlRequest(["audit"]))
        with pytest.raises(CapabilityUnsupportedError):
            await instance.get_note_detail("id")
        await instance.close()
    asyncio.run(run())


def test_mock_is_never_labeled_live_when_explicitly_allowed():
    driver = MockDriver({"allow_synthetic": True})
    response = asyncio.run(driver.crawl_keywords(CrawlRequest(["quoted / 中文"], 1)))
    assert response.is_synthetic and response.data_source != "live"
    assert all(n.is_synthetic and n.data_source != "live" for n in response.notes)
    assert "/" not in response.notes[0].note_id


def test_repeated_discovery_events_preserve_time_and_rank():
    note = UnifiedNote("id")
    note.add_discovery("kw", rank=1, discovered_at=100)
    note.add_discovery("kw", rank=1, discovered_at=200)
    note.add_discovery("kw", rank=2, discovered_at=300)
    note.add_discovery("kw", rank=1, discovered_at=100)
    assert [(d.rank, d.discovered_at) for d in note.discovery_records] == [(1, 100), (1, 200), (2, 300)]


@pytest.mark.parametrize("body", [None, "MISSING"])
def test_jsonl_resume_uses_missing_presence_and_sanitizes_media(tmp_path, body):
    raw = {"note_id": "id", "image_list": ["https://example.invalid/i?access_token=FAKE"], "video_url": None}
    if body is None:
        raw["desc"] = None
    file = tmp_path / "data.jsonl"
    file.write_text(json.dumps(raw) + "\n", encoding="utf-8")
    pipeline = XhsPipeline.__new__(XhsPipeline)
    pipeline.cleaner = DataCleaner()
    note = pipeline.load_from_jsonl(str(file))[0]
    assert note.desc == "" and note.desc_presence == FieldPresence.NOT_FETCHED
    assert note.media_presence == FieldPresence.VALID
    assert "FAKE" not in str(note.fact)


def test_shorter_newer_full_detail_is_not_discarded():
    old = UnifiedNote("id", desc="old complete version is long", crawled_at=100)
    new = UnifiedNote("id", desc="new", crawled_at=200)
    note = DataCleaner.deduplicate_notes([old, new])[0]
    assert note.desc == "new"
    partial = UnifiedNote("id", desc="very long partial text" * 10, completeness=ContentCompleteness.TRUNCATED)
    assert DataCleaner.deduplicate_notes([note, partial])[0].desc == "new"


def test_real_mediacrawler_sort_and_media_config_are_applied(tmp_path):
    folder = tmp_path / "config"
    folder.mkdir()
    base = folder / "base_config.py"
    base.write_text('KEYWORDS="old"\nENABLE_GET_MEDIA=False\nfrom .xhs_config import *\n', encoding="utf-8")
    xhs = folder / "xhs_config.py"
    xhs.write_text('SORT_TYPE="popularity_descending"\n', encoding="utf-8")
    driver = MediaCrawlerDriver({"mediacrawler_path": str(tmp_path)})
    driver._configure_mediacrawler(CrawlRequest(["audit"], enable_media=True, sort_type="latest"))
    def value(file, key):
        for node in ast.parse(file.read_text(encoding="utf-8")).body:
            if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == key for t in node.targets):
                return ast.literal_eval(node.value)
    assert value(base, "ENABLE_GET_MEDIA") is True
    assert value(xhs, "SORT_TYPE") == "time_descending"


def test_unfetched_default_and_null_body_are_not_claimed_empty_or_full(tmp_path):
    note = UnifiedNote("id")
    assert note.desc_presence == FieldPresence.NOT_FETCHED
    assert note.media_presence == FieldPresence.NOT_FETCHED
    assert note.completeness.value == "unknown"
    file = tmp_path / "data.jsonl"
    file.write_text('{"note_id":"id","desc":null}\n', encoding="utf-8")
    pipeline = XhsPipeline.__new__(XhsPipeline)
    pipeline.cleaner = DataCleaner()
    assert pipeline.load_from_jsonl(str(file))[0].completeness.value == "unknown"


def test_cached_simulation_requires_explicit_consent(tmp_path):
    file = tmp_path / "data.jsonl"
    file.write_text('{"note_id":"id","desc":"demo","is_synthetic":true,"data_source":"mock"}\n', encoding="utf-8")
    pipeline = XhsPipeline.__new__(XhsPipeline)
    pipeline.cleaner = DataCleaner()
    with pytest.raises(CapabilityUnsupportedError):
        pipeline.load_from_jsonl(str(file))
    assert pipeline.load_from_jsonl(str(file), allow_synthetic=True)[0].is_synthetic


def test_unsupported_live_detail_is_explicit():
    with pytest.raises(CapabilityUnsupportedError):
        asyncio.run(MediaCrawlerDriver().get_note_detail("id"))
