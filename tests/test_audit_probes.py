# -*- coding: utf-8 -*-
"""
Audit Probes Verification Suite
Directly runs the exact probe scenarios defined by the external audit (GPT).
"""

import os
import sys
import tempfile
import ast
import json
import asyncio
from pathlib import Path

root_dir = str(Path(__file__).resolve().parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from core.contracts import (
    NoteReference,
    CrawlRequest,
    CrawlResponse,
    UnifiedNote,
    EngagementMetrics,
    FieldPresence,
    ContentCompleteness,
    DiscoveryRecord,
    StageStatus,
    RateLimitError,
    ReferenceExpiredError
)
from core.checkpoint import CheckpointStore
from core.storage import ObsidianExporter, CsvExporter
from core.drivers.http_driver import HttpCrawlerDriver
from core.drivers.mediacrawler import MediaCrawlerDriver
from core.drivers.mock_driver import MockDriver
from core.normalizer import DataCleaner
from core.pipeline import XhsPipeline


def test_security_probes():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        store = CheckpointStore(str(tmp / "state"))
        ref = NoteReference(
            "audit_note", "audit", access_token="AUDIT_FAKE_TOKEN",
            access_context={"Cookie": "AUDIT_FAKE_COOKIE"}, can_resume_cross_process=False
        )
        saved = Path(store.save_references("audit", [ref])).read_text(encoding="utf-8")
        assert "AUDIT_FAKE_TOKEN" not in saved
        assert "AUDIT_FAKE_COOKIE" not in saved

        http = HttpCrawlerDriver()
        note = http._parse_raw_note({
            "note_id": "audit_url", "title": "Audit",
            "desc": "Synthetic text", "url": "https://example.invalid/n?xsec_token=AUDIT_FAKE_URL_TOKEN"
        })
        card, _ = ObsidianExporter(str(tmp / "vault")).export_note(note)
        csv = CsvExporter(str(tmp / "notes.csv")).export([note])
        assert "AUDIT_FAKE_URL_TOKEN" not in Path(card).read_text(encoding="utf-8")
        assert "AUDIT_FAKE_URL_TOKEN" not in Path(csv).read_text(encoding="utf-8-sig")

        note = UnifiedNote("audit_csv", title="=1+1", desc="@SUM(1,1)")
        csv = CsvExporter(str(tmp / "formula.csv")).export([note])
        csv_text = Path(csv).read_text(encoding="utf-8-sig")
        assert "'=1+1" in csv_text
        assert "'@SUM(1,1)" in csv_text

        cfg_dir = tmp / "crawler" / "config"
        cfg_dir.mkdir(parents=True)
        cfg_path = cfg_dir / "base_config.py"
        cfg_path.write_text('KEYWORDS = "old"\nCRAWLER_MAX_NOTES_COUNT = 5\n', encoding="utf-8")
        driver = MediaCrawlerDriver({"mediacrawler_path": str(cfg_dir.parent)})
        driver._configure_mediacrawler(CrawlRequest(keywords=['safe"\nAUDIT_INJECTED_STATEMENT = True\n#']))
        tree = ast.parse(cfg_path.read_text(encoding="utf-8"))
        injected = any(
            isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "AUDIT_INJECTED_STATEMENT" for t in n.targets)
            for n in tree.body
        )
        assert not injected
    print("[√] Security probe assertions verified.")


def test_data_contract_probes():
    http = HttpCrawlerDriver({})
    # Zero count must be VALID and 0, not None / NOT_FETCHED!
    n = http._parse_raw_note({'note_id': 'zero', 'liked_count': 0})
    assert n.metrics.likes == 0 and n.metrics.likes_presence == FieldPresence.VALID

    # Missing desc must be NOT_FETCHED, missing media must be NOT_FETCHED!
    n = http._parse_raw_note({'note_id': 'missing'})
    assert n.desc_presence == FieldPresence.NOT_FETCHED
    assert n.media_presence == FieldPresence.NOT_FETCHED

    # Token in URL must be stripped!
    n = http._parse_raw_note({'note_id': 'token', 'url': 'https://www.xiaohongshu.com/explore/token?xsec_token=TEST_ONLY_FAKE'})
    assert 'xsec_token' not in n.fact.source_url
    assert 'xsec_token' not in n.to_dict()['url']

    # Merge upgrades truncated snippet to full text and metrics, and preserves discovered_at
    snippet = UnifiedNote('shared', title='snippet', desc='short', completeness=ContentCompleteness.TRUNCATED)
    snippet.discovery_records.append(DiscoveryRecord(keyword='a', rank=1, discovered_at=100))
    detail = UnifiedNote('shared', title='full title', desc='full text', metrics=EngagementMetrics.from_raw(300), video_url='video', completeness=ContentCompleteness.FULL)
    detail.discovery_records.append(DiscoveryRecord(source_type='direct_url', discovered_at=200))
    merged = DataCleaner.deduplicate_notes([snippet, detail])[0]

    assert merged.desc == 'full text'
    assert merged.metrics.likes == 300
    assert merged.video_url == 'video'
    assert merged.title == 'full title'
    assert merged.discovery_records[1].discovered_at == 200

    # Content hash collision: different content arrangements must have different hashes
    one, two = UnifiedNote('hash', title='a|b', desc='c'), UnifiedNote('hash', title='a', desc='b|c')
    assert one.content_hash != two.content_hash

    # MediaCrawler detail query capability must declare False
    m = MediaCrawlerDriver({'mediacrawler_path': 'unused'})
    assert m.capabilities.can_get_detail is False

    # MockDriver detail query with reference attaches discovery info
    async def _test_detail():
        mock = MockDriver()
        detail = await mock.get_note_detail(NoteReference('id', 'mock', discovered_keyword='searched', rank=1))
        assert detail.primary_keyword == 'searched'
        assert len(detail.discovery_records) == 1
    asyncio.run(_test_detail())

    print("[√] Data contract probe assertions verified.")


def test_persistence_probes():
    with tempfile.TemporaryDirectory() as tmpdir:
        folder = Path(tmpdir)
        exporter = ObsidianExporter(str(folder / 'vault'))
        store = CheckpointStore(str(folder / 'state'))

        # 1. Byte-level LF writing: written hash matches disk bytes
        n = UnifiedNote(note_id='committed01', title='title', desc='original')
        path, h = exporter.export_note(n)
        store.commit_export('task', n.note_id, path, source_hash=n.content_hash, artifact_hash=h)
        import hashlib
        assert h == hashlib.sha256(Path(path).read_bytes()).hexdigest()

        # 2. Deleted file on disk is NOT marked EXPORTED
        Path(path).unlink()
        fresh_n = UnifiedNote(note_id=n.note_id, title=n.title, desc=n.desc)
        stage = store.reconcile_after_crash('task', exporter.vault_dir, [fresh_n])
        assert stage[n.note_id] != StageStatus.EXPORTED

        # 3. Input note changed is NOT marked EXPORTED
        path, h = exporter.export_note(n)
        store.commit_export('task', n.note_id, path, source_hash=n.content_hash, artifact_hash=h)
        updated = UnifiedNote(note_id=n.note_id, title=n.title, desc='new version')
        stage = store.reconcile_after_crash('task', exporter.vault_dir, [updated])
        assert stage[n.note_id] != StageStatus.EXPORTED

        # 4. Truncated / uncommitted file with only content_hash line is NOT marked EXPORTED
        n_tamper = UnifiedNote(note_id='tampered01', title='title', desc='original')
        path_t, h_t = exporter.export_note(n_tamper)
        Path(path_t).write_text(f'content_hash: "{n_tamper.content_hash}"\nTRUNCATED\n', encoding='utf-8')
        stage = store.reconcile_after_crash('task', exporter.vault_dir, [n_tamper])
        assert stage[n_tamper.note_id] != StageStatus.EXPORTED

        # 5. User annotations: source desc containing duplicate header does NOT destroy human notes
        header = exporter.USER_NOTES_HEADER
        n_ann = UnifiedNote(note_id='annotations01', title='title', desc='body\n' + header + '\nsource continuation')
        path_ann, _ = exporter.export_note(n_ann)
        with open(path_ann, 'a', encoding='utf-8') as f:
            f.write('\nUNIQUE HUMAN ANNOTATION\n')
        exporter.export_note(n_ann)
        assert 'UNIQUE HUMAN ANNOTATION' in Path(path_ann).read_text(encoding='utf-8')

        # 6. Read failure protects user notes from being overwritten
        real_open = open
        def failing_open(filename, mode='r', *args, **kwargs):
            if str(filename) == path_ann and mode == 'r':
                raise OSError('injected transient read failure')
            return real_open(filename, mode, *args, **kwargs)
        from unittest.mock import patch
        with patch('builtins.open', side_effect=failing_open):
            try:
                exporter.export_note(n_ann)
            except OSError:
                pass
        assert 'UNIQUE HUMAN ANNOTATION' in Path(path_ann).read_text(encoding='utf-8')

        # 7. True idempotence: unchanged cards are not rewritten
        n_rep = UnifiedNote(note_id='repeat01', title='title', desc='body')
        pipe = XhsPipeline.__new__(XhsPipeline)
        pipe.settings = {}
        pipe.checkpoint_store = store
        path_rep = pipe.export_obsidian_cards([n_rep], exporter.vault_dir, 'task')[0]
        calls = []
        real_replace = os.replace
        def watch_replace(src, dst):
            calls.append(str(dst))
            return real_replace(src, dst)
        with patch('core.storage.os.replace', side_effect=watch_replace):
            pipe.export_obsidian_cards([n_rep], exporter.vault_dir, 'task')
        assert len(calls) == 0, "Identical card export must skip os.replace"

    print("[√] Persistence probe assertions verified.")


if __name__ == "__main__":
    test_security_probes()
    test_data_contract_probes()
    test_persistence_probes()
    print("\n[√] ALL AUDIT PROBES VERIFIED AND PASSED 100%!")
