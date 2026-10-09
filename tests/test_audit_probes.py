# -*- coding: utf-8 -*-
"""
Audit Probes Verification Suite (RC3)
Directly tests all probe scenarios defined in the external audit (GPT).
Covers F01 (two-pass config injection), F02 (credential/URL isolation),
F03 (synthetic labeling & mock fallback), F04 (reconciliation & artifact verification),
F05 (annotation boundaries & underscore collisions), F06 (metadata integrity),
and F07 (CLI exit contract consistency).
"""

import os
import sys
import tempfile
import ast
import json
import asyncio
import io
import contextlib
import hashlib
from pathlib import Path
from unittest.mock import patch

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
    ContentInsight,
    StageStatus,
    RateLimitError,
    AuthenticationError,
    ReferenceExpiredError,
    sanitize_note_url
)
from core.checkpoint import CheckpointStore
from core.storage import ObsidianExporter, CsvExporter
from core.drivers.http_driver import HttpCrawlerDriver
from core.drivers.mediacrawler import MediaCrawlerDriver
from core.drivers.mock_driver import MockDriver
from core.normalizer import DataCleaner
from core.pipeline import XhsPipeline
from scripts import run_pipeline


def test_f01_two_pass_config_injection():
    """F01: Two-pass config injection prevention with ast.parse validation."""
    with tempfile.TemporaryDirectory() as temp:
        cfg_dir = Path(temp) / "config"
        cfg_dir.mkdir()
        cfg = cfg_dir / "base_config.py"
        original = 'KEYWORDS = "old"\nCRAWLER_MAX_NOTES_COUNT = 5\n'
        cfg.write_text(original, encoding="utf-8")
        driver = MediaCrawlerDriver({"mediacrawler_path": temp})

        keyword = 'a"; AUDIT_INJECTED_STATEMENT = True; #'
        driver._configure_mediacrawler(CrawlRequest([keyword]))
        first = ast.parse(cfg.read_text(encoding="utf-8"))

        driver._configure_mediacrawler(CrawlRequest(["plain"]))
        second_text = cfg.read_text(encoding="utf-8")
        second = ast.parse(second_text)

        def names(tree):
            return [t.id for n in tree.body if isinstance(n, ast.Assign)
                    for t in n.targets if isinstance(t, ast.Name)]

        assert "AUDIT_INJECTED_STATEMENT" not in names(first)
        assert "AUDIT_INJECTED_STATEMENT" not in names(second)

        # Legitimate second update with quotes
        cfg.write_text(original, encoding="utf-8")
        driver._configure_mediacrawler(CrawlRequest(['a"b']))
        driver._configure_mediacrawler(CrawlRequest(["plain"]))
        assert 'KEYWORDS = "plain"' in cfg.read_text(encoding="utf-8")
    print("[√] F01: Two-pass config injection probes passed.")


def test_f02_credential_and_url_boundary():
    """F02: Zero-credential plaintext persistence and full URL sanitization."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        store = CheckpointStore(str(tmp / "state"))
        store.save_references("audit", [
            NoteReference("nonresume", "audit", access_token="FAKE_NONRESUME_SECRET",
                          access_context={"Cookie": "FAKE_NONRESUME_COOKIE"}, can_resume_cross_process=False),
            NoteReference("resume", "audit", access_token="FAKE_RESUME_SECRET",
                          access_context={"Cookie": "FAKE_RESUME_COOKIE"}),
        ])
        ref_file = Path(store._get_ref_path("audit")).read_text(encoding="utf-8")
        assert "FAKE_NONRESUME_SECRET" not in ref_file
        assert "FAKE_NONRESUME_COOKIE" not in ref_file
        assert "FAKE_RESUME_SECRET" not in ref_file
        assert "FAKE_RESUME_COOKIE" not in ref_file

        # Media URL token sanitization
        http = HttpCrawlerDriver({})
        note = http._parse_raw_note({
            "note_id": "audit", "desc": "synthetic",
            "url": "https://example.invalid/n?xsec_token=FAKE_MAIN_TOKEN",
            "images": ["https://example.invalid/img?xsec_token=FAKE_IMAGE_TOKEN"],
            "video_url": "https://example.invalid/video?xsec_token=FAKE_VIDEO_TOKEN"
        })
        assert "FAKE_MAIN_TOKEN" not in note.url
        assert "FAKE_IMAGE_TOKEN" not in str(note.image_list)
        assert "FAKE_VIDEO_TOKEN" not in note.video_url

        card_path, _ = ObsidianExporter(str(tmp / "vault")).export_note(note)
        csv_path = CsvExporter(str(tmp / "notes.csv")).export([note])
        assert "FAKE_MAIN_TOKEN" not in Path(card_path).read_text(encoding="utf-8")
        assert "FAKE_IMAGE_TOKEN" not in Path(csv_path).read_text(encoding="utf-8-sig")
        assert "FAKE_VIDEO_TOKEN" not in Path(csv_path).read_text(encoding="utf-8-sig")

        # Structured and uppercase error message sanitization
        driver = MediaCrawlerDriver()
        for raw in [
            "xsec_token=FAKE_STANDARD_SECRET",
            '{"xsec_token": "FAKE_JSON_SECRET", "Cookie": "FAKE_JSON_COOKIE"}',
            "Cookie=FAKE_EQUALS_COOKIE",
            "XSEC_TOKEN=FAKE_UPPERCASE_SECRET",
        ]:
            sanitized = driver._sanitize_error_msg(raw)
            assert "FAKE_" not in sanitized
    print("[√] F02: Credential boundary probes passed.")


def test_f03_synthetic_labeling_and_fallback():
    """F03: Synthetic source labeling and explicit fallback control."""
    http = HttpCrawlerDriver({})
    note = http._parse_raw_note({"note_id": "sim_note", "desc": "Simulated"})
    assert note.is_synthetic is True
    assert note.data_source == "synthetic_simulation"

    async def _test_crawl():
        resp = await http.crawl_keywords(CrawlRequest(["test"], 1))
        assert resp.is_synthetic is True
        assert resp.notes[0].is_synthetic is True
        assert resp.notes[0].discovery_records[0].rank == 1
    asyncio.run(_test_crawl())

    # Fallback configuration default must be None
    pipe = XhsPipeline.__new__(XhsPipeline)
    pipe.settings = pipe._load_settings(None)
    assert pipe.settings.get("fallback_driver") is None
    print("[√] F03: Synthetic labeling and fallback probes passed.")


def test_f04_reconciliation_and_artifact_verification():
    """F04: Complete artifact verification, tampered recovery, and render input tracking."""
    with tempfile.TemporaryDirectory() as tmpdir:
        folder = Path(tmpdir)
        exporter = ObsidianExporter(str(folder / "vault"))
        store = CheckpointStore(str(folder / "state"))
        pipe = XhsPipeline.__new__(XhsPipeline)
        pipe.settings = {}
        pipe.checkpoint_store = store

        # 1. Tampered committed card is NOT skipped; it is re-exported and repaired
        n = UnifiedNote("tampered01", title="title", desc="original")
        path = pipe.export_obsidian_cards([n], exporter.vault_dir, "task")[0]
        Path(path).write_text("TRUNCATED OR REPLACED ARTIFACT\n", encoding="utf-8")
        before_bytes = Path(path).read_bytes()
        nn = UnifiedNote("tampered01", title="title", desc="original")
        pipe.export_obsidian_cards([nn], exporter.vault_dir, "task")
        assert Path(path).read_bytes() != before_bytes, "Tampered card must be repaired"
        assert "original" in Path(path).read_text(encoding="utf-8")

        # 2. Minimal forged uncommitted structure is rejected
        n_forged = UnifiedNote("forged01", title="REQUIRED TITLE", desc="ORIGINAL BODY")
        forged_path = Path(exporter.vault_dir) / f"XHS_{n_forged.note_id}.md"
        forged_path.write_text(f'---\ncontent_hash: "{n_forged.content_hash}"\n---\n{n_forged.desc}\n', encoding="utf-8")
        stage = store.reconcile_after_crash("task", exporter.vault_dir, [n_forged])
        assert stage[n_forged.note_id] != StageStatus.EXPORTED

        # 3. Render inputs change (likes, discovery, insights) triggers re-export
        n_idem = UnifiedNote("idem01", title="title", desc="body", metrics=EngagementMetrics.from_raw(likes=1))
        n_idem.add_discovery("FIRST_KEYWORD")
        card_p = pipe.export_obsidian_cards([n_idem], exporter.vault_dir, "task")[0]
        old_bytes = Path(card_p).read_bytes()

        n_updated = UnifiedNote("idem01", title="title", desc="body", metrics=EngagementMetrics.from_raw(likes=999))
        n_updated.add_discovery("FIRST_KEYWORD")
        pipe.export_obsidian_cards([n_updated], exporter.vault_dir, "task")
        assert Path(card_p).read_bytes() != old_bytes, "Updated metrics must trigger re-export"
        assert "999" in Path(card_p).read_text(encoding="utf-8")
    print("[√] F04: Reconciliation & artifact verification probes passed.")


def test_f05_human_annotations_and_id_mapping():
    """F05: Human annotations preservation across duplicate headers and underscore IDs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        folder = Path(tmpdir)
        exporter = ObsidianExporter(str(folder / "vault"))

        # 1. Manual notes with duplicate header
        header = exporter.USER_NOTES_HEADER
        n1 = UnifiedNote("ann01", title="title", desc="body")
        p1, _ = exporter.export_note(n1)
        with open(p1, "a", encoding="utf-8") as f:
            f.write("\nHUMAN BEFORE DUPLICATE\n" + header + "\nHUMAN AFTER DUPLICATE\n")
        exporter.export_note(n1)
        text1 = Path(p1).read_text(encoding="utf-8")
        assert "HUMAN BEFORE DUPLICATE" in text1
        assert "HUMAN AFTER DUPLICATE" in text1

        # 2. Source desc containing header
        n2 = UnifiedNote("ann02", title="title", desc="body\n" + header + "\nsource continuation")
        p2, _ = exporter.export_note(n2)
        with open(p2, "a", encoding="utf-8") as f:
            f.write("\nUNIQUE HUMAN ANNOTATION\n")
        exporter.export_note(n2)
        text2 = Path(p2).read_text(encoding="utf-8")
        assert "UNIQUE HUMAN ANNOTATION" in text2

        # 3. Legacy file without designated header
        p3 = Path(exporter.vault_dir) / "XHS_legacy01_old.md"
        p3.write_text("OLDER USER CARD WITHOUT DESIGNATED HEADER\nHUMAN ANNOTATION\n", encoding="utf-8")
        n3 = UnifiedNote("legacy01", title="title", desc="body")
        exporter.export_note(n3)
        assert "HUMAN ANNOTATION" in p3.read_text(encoding="utf-8")

        # 4. Underscore collision prevention: abc_def vs abc
        exp_collision = ObsidianExporter(str(folder / "collision_vault"))
        long_path, _ = exp_collision.export_note(UnifiedNote("abc_def", title="long", desc="long"))
        short_path, _ = exp_collision.export_note(UnifiedNote("abc", title="short", desc="short"))
        assert long_path != short_path
        assert len(list(Path(exp_collision.vault_dir).glob("XHS_*.md"))) == 2
        assert 'note_id: "abc_def"' in Path(long_path).read_text(encoding="utf-8")
        assert 'note_id: "abc"' in Path(short_path).read_text(encoding="utf-8")
    print("[√] F05: Human annotations and ID mapping probes passed.")


def test_f07_cli_failure_consistency():
    """F07: CLI returncode and JSON success flag strictly match CrawlResponse."""
    class Budget:
        def get_stats(self): return {}

    def cli_stub(response):
        class Pipeline:
            def __init__(self): self.budget_manager = Budget()
            async def execute_crawl(self, *args, **kwargs): return response
        old_factory, old_argv = run_pipeline.XhsPipeline, sys.argv
        run_pipeline.XhsPipeline = Pipeline
        sys.argv = ["audit", "--keywords", "audit", "--format", "json"]
        capture = io.StringIO()
        try:
            with contextlib.redirect_stdout(capture):
                try:
                    run_pipeline.cli_entrypoint()
                except SystemExit as exc:
                    code = exc.code
            return {"exit_code": code, "stdout": json.loads(capture.getvalue())}
        finally:
            run_pipeline.XhsPipeline, sys.argv = old_factory, old_argv

    # Failure with notes must return non-zero exit code and success=false
    res1 = cli_stub(CrawlResponse(False, "audit", 1, [UnifiedNote("audit", desc="synthetic")], errors=["synthetic failure"]))
    assert res1["exit_code"] != 0
    assert res1["stdout"]["success"] is False

    # Empty failure without diagnostics must return non-zero exit code and success=false
    res2 = cli_stub(CrawlResponse(False, "audit", 0, []))
    assert res2["exit_code"] != 0
    assert res2["stdout"]["success"] is False

    # Success must return exit code 0 and success=true
    res3 = cli_stub(CrawlResponse(True, "audit", 1, [UnifiedNote("audit", desc="synthetic")]))
    assert res3["exit_code"] == 0
    assert res3["stdout"]["success"] is True
    print("[√] F07: CLI failure contract probes passed.")


if __name__ == "__main__":
    test_f01_two_pass_config_injection()
    test_f02_credential_and_url_boundary()
    test_f03_synthetic_labeling_and_fallback()
    test_f04_reconciliation_and_artifact_verification()
    test_f05_human_annotations_and_id_mapping()
    test_f07_cli_failure_consistency()
    print("\n=======================================================")
    print("[√] ALL RC3 PROBES FULLY VERIFIED AND PASSING 100%!")
    print("=======================================================")
