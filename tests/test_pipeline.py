# -*- coding: utf-8 -*-
"""
xhs-pipeline Test Suite (v0.3.0-aligned)
Verifies:
1. Four-way version synchronization (VERSION, pyproject.toml, core.__version__, CHANGELOG.md).
2. Explicit metric parsing with FieldPresence semantics.
3. Multi-keyword deduplication & trail merging (DataCleaner).
4. Direct note_id detail query without dummy keywords.
5. Resilience mechanisms (RequestBudgetManager, CircuitBreaker, DriverFallbackRouter).
6. Atomic file write & crash consistency reconciliation (CheckpointStore).
7. Research reproducibility manifest (CrawlTaskManifest).
8. Idempotent Obsidian export with non-destructive user note preservation.
9. Multi-driver pipeline execution (MockDriver & HttpCrawlerDriver).
"""

import os
import sys
import tempfile
import asyncio
from pathlib import Path

# Setup path
root_dir = str(Path(__file__).resolve().parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from core import (
    __version__,
    UnifiedNote,
    ContentFact,
    EngagementMetrics,
    ContentCompleteness,
    FieldPresence,
    ContentInsight,
    DiscoveryRecord,
    NoteReference,
    CrawlRequest,
    CrawlTaskManifest,
    StageStatus,
    XhsPipeline,
    MockDriver,
    HttpCrawlerDriver,
    DataCleaner,
    ObsidianExporter,
    CsvExporter,
    CheckpointStore,
    RequestBudgetManager,
    CircuitBreaker,
    DriverFallbackRouter,
    BudgetExceededError,
    CircuitBreakerOpenError
)
from core.contracts import parse_metric_count


def test_version_consistency():
    """Verify four-way version synchronization across static and runtime artifacts."""
    version_file = Path(root_dir) / "VERSION"
    assert version_file.exists(), "VERSION file must exist"
    file_version = version_file.read_text(encoding="utf-8").strip()

    assert file_version == __version__, f"VERSION file ({file_version}) != core.__version__ ({__version__})"
    assert __version__ == "0.3.2rc3", f"Expected version 0.3.2rc3, got {__version__}"

    pyproject_file = Path(root_dir) / "pyproject.toml"
    assert 'version = "0.3.2rc3"' in pyproject_file.read_text(encoding="utf-8")

    changelog_file = Path(root_dir) / "CHANGELOG.md"
    assert '## [0.3.2rc3]' in changelog_file.read_text(encoding="utf-8")
    print("[√] Version consistency verified (0.3.2rc3).")


def test_field_presence_and_metric_parsing():
    """Verify Chinese social metrics parsing distinguishes valid numbers from uncollected states."""
    num, raw, is_app, pres = parse_metric_count("8.7万")
    assert num == 87000
    assert raw == "8.7万"
    assert is_app is True
    assert pres == FieldPresence.VALID

    num, raw, is_app, pres = parse_metric_count("0")
    assert num == 0
    assert raw == "0"
    assert pres == FieldPresence.VALID

    # None must be FieldPresence.NOT_FETCHED rather than 0!
    num, raw, is_app, pres = parse_metric_count(None)
    assert num is None
    assert pres == FieldPresence.NOT_FETCHED

    metrics = EngagementMetrics.from_raw(likes="100", comments=None)
    assert metrics.likes == 100
    assert metrics.likes_presence == FieldPresence.VALID
    assert metrics.comments is None
    assert metrics.comments_presence == FieldPresence.NOT_FETCHED, "Uncollected comments must not be faked as 0"
    print("[√] FieldPresence & metric semantics verified.")


def test_multi_keyword_deduplication():
    """Verify acceptance scenario: Same note discovered via multiple keywords merges trails without duplicating."""
    cleaner = DataCleaner()
    note_kw1 = UnifiedNote(
        note_id="shared_note_001",
        title="杭州西湖纯干货",
        desc="西湖游览全攻略",
        author_name="旅人",
        metrics=EngagementMetrics.from_raw("1000", "500")
    )
    note_kw1.add_discovery(keyword="杭州旅游", rank=1)

    note_kw2 = UnifiedNote(
        note_id="shared_note_001",
        title="杭州西湖纯干货",
        desc="西湖游览全攻略",
        author_name="旅人",
        metrics=EngagementMetrics.from_raw("1000", "500")
    )
    note_kw2.add_discovery(keyword="西湖美食", rank=5)

    deduped = cleaner.deduplicate_notes([note_kw1, note_kw2])
    assert len(deduped) == 1, "Duplicate notes must be deduplicated to exactly 1 instance"
    merged = deduped[0]
    assert len(merged.discovery_records) == 2, "Both discovery pathways must be retained"
    keywords = [d.keyword for d in merged.discovery_records]
    assert "杭州旅游" in keywords and "西湖美食" in keywords
    print("[√] Multi-keyword deduplication & trail merging verified.")


def test_direct_note_lookup_no_fake_keyword():
    """Verify acceptance scenario: Direct detail query by note_id does not require dummy keywords."""
    note = UnifiedNote(
        note_id="direct_id_999",
        title="通过ID直查的笔记",
        desc="该笔记直接通过URL或ID获取，无任何检索来源"
    )
    assert note.primary_keyword == "", "Direct lookup note must not have dummy keyword"
    assert len(note.discovery_records) == 0

    note.add_discovery(source_type="direct_url")
    assert note.discovery_records[0].source_type == "direct_url"
    assert note.discovery_records[0].keyword is None
    print("[√] Direct note detail lookup without dummy keywords verified.")


def test_atomic_export_and_crash_reconciliation():
    """Verify acceptance scenario: File write followed by crash can be reconciled without duplicate writes."""
    with tempfile.TemporaryDirectory() as tmpdir:
        vault_dir = os.path.join(tmpdir, "vault")
        state_dir = os.path.join(tmpdir, "state")
        exporter = ObsidianExporter(vault_dir=vault_dir)
        checkpoint_store = CheckpointStore(state_dir=state_dir)

        note = UnifiedNote(
            note_id="crash_test_01",
            title="崩溃恢复测试笔记",
            desc="正文内容",
            metrics=EngagementMetrics.from_raw("500")
        )

        # 1. File write succeeds atomically
        file_path, written_hash = exporter.export_note(note)
        assert os.path.exists(file_path)

        # 2. Simulate process crash BEFORE committing state!
        # Notice: checkpoint_store has not called commit_export yet.

        # 3. Process restarts, performs crash reconciliation
        notes = [note]
        reconciled_stages = checkpoint_store.reconcile_after_crash("task_01", vault_dir, notes)
        assert reconciled_stages["crash_test_01"] == StageStatus.EXPORTED
        assert note.stage_status == StageStatus.EXPORTED

        # Verify that card was not duplicated
        cards = list(Path(vault_dir).glob("XHS_crash_test_01*.md"))
        assert len(cards) == 1, "Crash recovery must reconcile in-place without generating duplicates"
    print("[√] Atomic export & crash consistency reconciliation verified.")


def test_research_reproducibility_manifest():
    """Verify acceptance scenario: CrawlTaskManifest records complete search criteria and coverage."""
    async def _run():
        pipeline = XhsPipeline()
        req = CrawlRequest(
            keywords=["科研复现", "杭州"],
            max_count_per_keyword=2,
            sort_type="popularity_descending"
        )
        res = await pipeline.execute_crawl(req, driver_name="mock")
        assert res.manifest is not None
        m: CrawlTaskManifest = res.manifest

        assert m.task_id == req.task_id
        assert m.driver_name == "mock"
        assert m.keywords == ["科研复现", "杭州"]
        assert m.sort_type == "popularity_descending"
        assert m.actual_discovered_count > 0
        assert m.deduplicated_count > 0
        assert len(m.content_hashes) == m.deduplicated_count
        assert m.stop_reason == "completed"

        # Verify manifest was saved to disk
        loaded_m = pipeline.checkpoint_store.load_task_manifest(req.task_id)
        assert loaded_m is not None
        assert loaded_m.task_id == req.task_id
        assert loaded_m.keywords == req.keywords

    asyncio.run(_run())
    print("[√] Research reproducibility manifest verified.")


def test_resilience_budget_and_breaker():
    """Verify RequestBudgetManager, CircuitBreaker state transitions, and driver failover."""
    budget = RequestBudgetManager(max_budget=2, rate_limit_per_min=60)
    async def run_budget():
        await budget.acquire()
        await budget.acquire()
        try:
            await budget.acquire()
            assert False, "Should have raised BudgetExceededError"
        except BudgetExceededError:
            pass
    asyncio.run(run_budget())
    assert budget.used_requests == 2

    cb = CircuitBreaker("test_breaker", failure_threshold=2, recovery_timeout=0.1)
    assert cb.can_execute() is True
    cb.record_failure()
    assert cb.can_execute() is True
    cb.record_failure()
    assert cb.can_execute() is False

    router = DriverFallbackRouter(primary_driver="mediacrawler", fallback_driver="mock")
    p_cb = router.get_circuit_breaker("mediacrawler")
    p_cb.record_failure()
    p_cb.record_failure()
    p_cb.record_failure()
    assert p_cb.can_execute() is False

    active = router.select_healthy_driver()
    assert active == "mock"
    print("[√] Resilience budget, circuit breaker & failover verified.")


def test_idempotent_obsidian_preservation():
    """Verify non-destructive Obsidian updates: user manual notes are preserved upon re-export."""
    pipeline = XhsPipeline()
    note = UnifiedNote(
        note_id="test_preserve_01",
        title="西湖避坑指南",
        desc="第一版原始正文事实",
        author_id="author_hash_1",
        author_name="测试博主",
        metrics=EngagementMetrics.from_raw("1.2万", "5000", "300", "50"),
        url="https://www.xiaohongshu.com/explore/test_preserve_01"
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        files = pipeline.export_obsidian_cards([note], tmpdir)
        assert len(files) == 1
        card_path = files[0]

        custom_user_annotation = "\n### 我的个人调研手记\n- 重点核实：这家店的熟醉虾确实新鲜，但周末排队超过40分钟。\n"
        with open(card_path, "a", encoding="utf-8") as f:
            f.write(custom_user_annotation)

        updated_note = UnifiedNote(
            note_id="test_preserve_01",
            title="西湖避坑指南 (最新版)",
            desc="第二版更新后的正文事实",
            author_id="author_hash_1",
            author_name="测试博主",
            metrics=EngagementMetrics.from_raw("1.5万", "6000", "400", "60"),
            url="https://www.xiaohongshu.com/explore/test_preserve_01"
        )

        files_v2 = pipeline.export_obsidian_cards([updated_note], tmpdir)
        assert len(files_v2) == 1

        content_v2 = Path(card_path).read_text(encoding="utf-8")
        assert 'likes_num: 15000' in content_v2
        assert '西湖避坑指南 (最新版)' in content_v2
        assert "我的个人调研手记" in content_v2
        assert "周末排队超过40分钟" in content_v2
    print("[√] Non-destructive idempotent Obsidian sync verified.")


def test_multi_driver_execution():
    """Verify execution of both MockDriver and HttpCrawlerDriver in XhsPipeline."""
    async def _run():
        pipeline = XhsPipeline()
        req = CrawlRequest(keywords=["单元测试"], max_count_per_keyword=2)
        res_mock = await pipeline.execute_crawl(req, driver_name="mock")
        assert res_mock.success is True

        res_http = await pipeline.execute_crawl(req, driver_name="http")
        assert res_http.success is True
        assert len(res_http.notes) > 0
        assert res_http.driver_name == "http"

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "test.csv")
            pipeline.export_csv(res_http.notes, csv_path)
            assert os.path.exists(csv_path)

            cards = pipeline.export_obsidian_cards(res_http.notes, tmpdir)
            assert len(cards) > 0

    asyncio.run(_run())
    print("[√] Multi-driver pipeline execution verified.")


def test_config_injection_defense():
    """F01 Regression: Ensure keywords with quotes and newlines cannot inject Python AST statements."""
    import ast
    from core.drivers.mediacrawler import MediaCrawlerDriver
    with tempfile.TemporaryDirectory() as tmpdir:
        cfg_dir = Path(tmpdir) / "config"
        cfg_dir.mkdir(parents=True)
        base_cfg = cfg_dir / "base_config.py"
        base_cfg.write_text('KEYWORDS = "default"\nCRAWLER_MAX_NOTES_COUNT = 10\n', encoding="utf-8")

        driver = MediaCrawlerDriver({"mediacrawler_path": tmpdir})
        malicious_kw = 'safe"\nINJECTED_ATTACK = True\n#'
        driver._configure_mediacrawler(CrawlRequest(keywords=[malicious_kw]))

        tree = ast.parse(base_cfg.read_text(encoding="utf-8"))
        has_injection = any(
            isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "INJECTED_ATTACK" for t in n.targets)
            for n in tree.body
        )
        assert not has_injection, "Malicious keyword must not inject new Python assignment statements"
    print("[√] F01 Config injection defense verified.")


def test_credential_isolation_and_url_sanitization():
    """F02 Regression: Ensure non-resumable tokens are not saved to disk, and URLs are sanitized."""
    with tempfile.TemporaryDirectory() as tmpdir:
        store = CheckpointStore(state_dir=tmpdir)
        ref = NoteReference(
            note_id="sec_01",
            driver_name="test",
            access_token="SECRET_TOKEN_XYZ",
            access_context={"Cookie": "SESSION_SECRET"},
            can_resume_cross_process=False
        )
        saved_file = store.save_references("task_sec", [ref])
        content = Path(saved_file).read_text(encoding="utf-8")
        assert "SECRET_TOKEN_XYZ" not in content, "Non-resumable token must not be saved to disk"
        assert "SESSION_SECRET" not in content, "Non-resumable cookie must not be saved to disk"

        # URL token sanitization
        note = UnifiedNote(
            note_id="url_01",
            title="Test",
            url="https://www.xiaohongshu.com/explore/url_01?xsec_token=LEAKED_TOKEN&param=1#fragment"
        )
        assert "xsec_token" not in note.url
        assert note.url == "https://www.xiaohongshu.com/explore/url_01"
    print("[√] F02 Credential isolation & URL sanitization verified.")


def test_csv_formula_injection_defense():
    """F02/Probe: CSV export neutralizes spreadsheet formula injection (=, +, -, @)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        exporter = CsvExporter(os.path.join(tmpdir, "formula.csv"))
        note = UnifiedNote("formula_note", title="=1+1", desc="@SUM(A1:A10)")
        csv_file = exporter.export([note])
        content = Path(csv_file).read_text(encoding="utf-8-sig")
        assert "'=1+1" in content
        assert "'@SUM(A1:A10)" in content
    print("[√] CSV formula injection defense verified.")


def test_prefix_collision_defense():
    """F05 Regression: Note 'abc123' must not overwrite or match 'abc1234'."""
    with tempfile.TemporaryDirectory() as tmpdir:
        exporter = ObsidianExporter(vault_dir=tmpdir)
        note_long = UnifiedNote("abc1234", title="Long ID")
        path_long, _ = exporter.export_note(note_long)

        note_short = UnifiedNote("abc123", title="Short ID")
        path_short, _ = exporter.export_note(note_short)

        assert path_long != path_short, "Prefix collision: Short ID must not reuse long ID card"
        assert len(list(Path(tmpdir).glob("*.md"))) == 2
        assert "abc1234" in Path(path_long).read_text(encoding="utf-8")
        assert "abc123" in Path(path_short).read_text(encoding="utf-8")
    print("[√] F05 Prefix collision defense verified.")


def test_content_hash_collision_resistance():
    """Contracts: Collision resistance for title and desc serialization in content hash."""
    note1 = UnifiedNote("h1", title="a|b", desc="c")
    note2 = UnifiedNote("h2", title="a", desc="b|c")
    assert note1.content_hash != note2.content_hash, "title|desc delimiter must not cause hash collision"
    assert len(note1.content_hash) == 64, "content_hash must be full 64-character SHA-256"
    print("[√] Content hash collision resistance verified.")


if __name__ == "__main__":
    test_version_consistency()
    test_field_presence_and_metric_parsing()
    test_multi_keyword_deduplication()
    test_direct_note_lookup_no_fake_keyword()
    test_atomic_export_and_crash_reconciliation()
    test_research_reproducibility_manifest()
    test_resilience_budget_and_breaker()
    test_idempotent_obsidian_preservation()
    test_multi_driver_execution()
    test_config_injection_defense()
    test_credential_isolation_and_url_sanitization()
    test_csv_formula_injection_defense()
    test_prefix_collision_defense()
    test_content_hash_collision_resistance()
    print("\n[√] All 14 comprehensive unit, acceptance & regression tests passed successfully!")
