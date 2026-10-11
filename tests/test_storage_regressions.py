from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path

import pytest

from core.contracts import ContentInsight, PipelineError, UnifiedNote
from core.checkpoint import CheckpointStore
from core.pipeline import XhsPipeline
from core.storage import ObsidianExporter


def pipe(tmp_path):
    result = XhsPipeline.__new__(XhsPipeline)
    result.settings = {}
    result.checkpoint_store = CheckpointStore(str(tmp_path / "state"))
    return result


def test_concurrent_commit_transactions_keep_all_notes(tmp_path):
    store = CheckpointStore(str(tmp_path))
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: store.commit_export("task", str(i), "path", "source"), range(64)))
    saved = json.loads((tmp_path / "commits_task.json").read_text(encoding="utf-8"))
    assert set(saved) == {str(i) for i in range(64)}


@pytest.mark.parametrize("bad_id", ['..\\..\\escape', '../escape', 'bad\nname', 'bad"name'])
def test_filesystem_identifiers_cannot_escape_or_inject(tmp_path, bad_id):
    with pytest.raises((ValueError, PipelineError)):
        ObsidianExporter(str(tmp_path / "vault")).export_note(UnifiedNote(bad_id, desc="body"))
    with pytest.raises((ValueError, PipelineError)):
        CheckpointStore(str(tmp_path / "state")).commit_export(bad_id, "note", "path", "source")


def test_exact_filename_with_wrong_identity_is_never_overwritten(tmp_path):
    exporter = ObsidianExporter(str(tmp_path))
    target = tmp_path / "XHS_target.md"
    target.write_text('---\nnote_id: "different"\n---\nHUMAN CONTENT\n', encoding="utf-8")
    before = target.read_bytes()
    with pytest.raises(PipelineError):
        exporter.export_note(UnifiedNote("target", desc="body"))
    assert target.read_bytes() == before


def test_marker_literal_in_machine_fields_cannot_become_user_boundary(tmp_path):
    exporter = ObsidianExporter(str(tmp_path))
    note = UnifiedNote("id", title="title\n<!-- BEGIN_USER_NOTES -->\nspoof", desc="body")
    path, _ = exporter.export_note(note)
    with open(path, "a", encoding="utf-8") as output:
        output.write("\nREAL HUMAN COMMENT\n")
    exporter.export_note(note)
    assert "REAL HUMAN COMMENT" in Path(path).read_text(encoding="utf-8")
    assert Path(path).read_text(encoding="utf-8").count("<!-- BEGIN_USER_NOTES -->") == 1


def test_ambiguous_user_marker_refuses_overwrite_and_keeps_bytes(tmp_path):
    exporter = ObsidianExporter(str(tmp_path))
    note = UnifiedNote("id", desc="body")
    path, _ = exporter.export_note(note)
    with open(path, "a", encoding="utf-8") as output:
        output.write("\nCOMMENT BEFORE\n<!-- BEGIN_USER_NOTES -->\nCOMMENT AFTER\n")
    before = Path(path).read_bytes()
    with pytest.raises(PipelineError):
        exporter.export_note(note)
    assert Path(path).read_bytes() == before


def test_same_insight_id_with_changed_evidence_updates_card(tmp_path):
    pipeline = pipe(tmp_path)
    note = UnifiedNote("id", desc="body")
    note.add_insight(ContentInsight("same", "id", "hook", evidence_snippets=["OLD_EVIDENCE"]))
    path = pipeline.export_obsidian_cards([note], str(tmp_path / "vault"), "task")[0]
    note.insights[0].evidence_snippets = ["NEW_EVIDENCE"]
    pipeline.export_obsidian_cards([note], str(tmp_path / "vault"), "task")
    assert "NEW_EVIDENCE" in Path(path).read_text(encoding="utf-8")


def test_artifact_loss_and_manual_edits_are_preserved(tmp_path):
    pipeline = pipe(tmp_path)
    note = UnifiedNote("id", title="title", desc="body")
    path = Path(pipeline.export_obsidian_cards([note], str(tmp_path / "vault"), "task")[0])
    with path.open("a", encoding="utf-8") as output:
        output.write("\nUSER COMMENT\n")
    edited = path.read_bytes()
    pipeline.export_obsidian_cards([note], str(path.parent), "task")
    assert path.read_bytes() == edited  # Human-only edits do not trigger rewriting.
    path.write_text("UNRECOGNIZED HUMAN OR DAMAGED FILE\n", encoding="utf-8")
    damaged = path.read_bytes()
    pipeline.export_obsidian_cards([note], str(path.parent), "task")
    assert "UNRECOGNIZED HUMAN OR DAMAGED FILE" in path.read_text(encoding="utf-8")
    assert any(p.read_bytes() == damaged for p in path.parent.glob("*.bak"))


def test_forged_body_only_in_user_region_cannot_recover(tmp_path):
    pipeline = pipe(tmp_path)
    note = UnifiedNote("id", title="title", desc="UNIQUE_FACT_BODY")
    exporter = ObsidianExporter(str(tmp_path / "vault"))
    path, _ = exporter.export_note(note)
    text = Path(path).read_text(encoding="utf-8")
    text = text.replace("UNIQUE_FACT_BODY", "MISSING BODY", 1) + "\nUNIQUE_FACT_BODY\n"
    Path(path).write_text(text, encoding="utf-8")
    stage = pipeline.checkpoint_store.reconcile_after_crash("task", exporter.vault_dir, [note])
    assert stage["id"].value != "exported"


def test_unchanged_export_is_byte_identical_and_skip_replace(tmp_path, monkeypatch):
    pipeline = pipe(tmp_path)
    note = UnifiedNote("id", desc="body")
    path = Path(pipeline.export_obsidian_cards([note], str(tmp_path / "vault"), "task")[0])
    before = path.read_bytes()
    calls = []
    import os
    original = os.replace
    monkeypatch.setattr(os, "replace", lambda src, dst: (calls.append(str(dst)), original(src, dst))[1])
    pipeline.export_obsidian_cards([note], str(path.parent), "task")
    assert path.read_bytes() == before and not calls
    assert pipeline.checkpoint_store.get_commit("task", "id")["artifact_hash"] == hashlib.sha256(before).hexdigest()


def test_processes_share_the_commit_transaction_lock(tmp_path):
    import os
    import subprocess
    import sys
    program = ("from core.checkpoint import CheckpointStore; import sys; "
               "s=CheckpointStore(sys.argv[1]); "
               "[s.commit_export('task',sys.argv[2]+str(i),'path','source') for i in range(16)]")
    env = {**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])}
    workers = [subprocess.Popen([sys.executable, "-c", program, str(tmp_path), label], env=env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE) for label in ("A", "B", "C")]
    for worker in workers:
        stdout, stderr = worker.communicate(timeout=20)
        assert worker.returncode == 0, stderr.decode(errors="replace")
    saved = json.loads((tmp_path / "commits_task.json").read_text(encoding="utf-8"))
    assert len(saved) == 48
