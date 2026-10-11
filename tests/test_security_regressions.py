import ast
import json

import pytest

from core.checkpoint import CheckpointStore
from core.contracts import CrawlRequest, DriverError, NoteReference, UnifiedNote
from core.drivers.mediacrawler import MediaCrawlerDriver
from core.storage import CsvExporter, sanitize_csv_cell


@pytest.mark.parametrize("assignment", [
    'KEYWORDS = (\n    "old"\n)\n',
    'KEYWORDS: str = "old"\n',
    'KEYWORDS = """old\nmultiline"""\n',
])
def test_config_updates_real_ast_assignment_repeatedly(tmp_path, assignment):
    config = tmp_path / "config" / "base_config.py"
    config.parent.mkdir()
    config.write_text(assignment + 'CRAWLER_MAX_NOTES_COUNT = 1\n', encoding="utf-8")
    driver = MediaCrawlerDriver({"mediacrawler_path": str(tmp_path)})
    for keyword in ['a"; INJECTED = True; #', "正常\\路径", "plain"]:
        driver._configure_mediacrawler(CrawlRequest([keyword]))
        tree = ast.parse(config.read_text(encoding="utf-8"))
        values = {}
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        values[target.id] = ast.literal_eval(node.value)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                values[node.target.id] = ast.literal_eval(node.value)
        assert values["KEYWORDS"] == keyword
        assert "INJECTED" not in values


def test_config_does_not_rewrite_lookalike_inside_string(tmp_path):
    config = tmp_path / "config" / "base_config.py"
    config.parent.mkdir()
    config.write_text('MESSAGE = """\nKEYWORDS = untouched\n"""\nKEYWORDS = "old"\n', encoding="utf-8")
    MediaCrawlerDriver({"mediacrawler_path": str(tmp_path)})._configure_mediacrawler(CrawlRequest(["new"]))
    node = ast.parse(config.read_text(encoding="utf-8")).body[0]
    assert ast.literal_eval(node.value) == "\nKEYWORDS = untouched\n"


def test_duplicate_config_assignment_fails_without_replacing_file(tmp_path):
    config = tmp_path / "config" / "base_config.py"
    config.parent.mkdir()
    config.write_text('KEYWORDS="one"\nKEYWORDS="two"\n', encoding="utf-8")
    before = config.read_bytes()
    with pytest.raises(DriverError):
        MediaCrawlerDriver({"mediacrawler_path": str(tmp_path)})._configure_mediacrawler(CrawlRequest(["new"]))
    assert config.read_bytes() == before


def test_nested_reference_credentials_are_not_persisted_or_resumed(tmp_path):
    store = CheckpointStore(str(tmp_path))
    ref = NoteReference("id", "audit", access_token="FAKE_TOKEN", access_context={
        "headers": [{"Cookie": "FAKE_COOKIE"}], "metadata": {"secret": "FAKE_SECRET"},
    })
    file = store.save_references("task", [ref])
    text = open(file, encoding="utf-8").read()
    assert "FAKE_" not in text
    valid, expired = store.load_references("task")
    assert not valid and len(expired) == 1


def test_loading_legacy_reference_never_restores_plaintext_secrets(tmp_path):
    store = CheckpointStore(str(tmp_path))
    (tmp_path / "references_task.json").write_text(json.dumps([{
        "note_id": "id", "driver_name": "audit", "access_token": "FAKE_OLD",
        "access_context": {"headers": {"Cookie": "FAKE_OLD_COOKIE"}},
        "can_resume_cross_process": True,
    }]), encoding="utf-8")
    valid, expired = store.load_references("task")
    assert not valid and len(expired) == 1
    assert expired[0].access_token == "" and expired[0].access_context == {}
    assert "FAKE_" not in (tmp_path / "references_task.json").read_text(encoding="utf-8")


def test_configuration_snapshot_never_exposes_url_credentials_or_opaque_objects():
    from core.security import public_config
    class Opaque:
        def __repr__(self): return "FAKE_PRIVATE_VALUE"
    value = public_config({"endpoint": "https://user:FAKE_PASS@example.invalid/api?access_token=FAKE_TOKEN", "opaque": Opaque()})
    assert "FAKE_" not in json.dumps(value)


def test_mutated_media_and_note_urls_are_sanitized_at_export(tmp_path):
    note = UnifiedNote("id", desc="body")
    note.url = "https://example.invalid/n?XSEC_TOKEN=FAKE_MAIN"
    note.image_list.append("https://example.invalid/img?width=320&access_token=FAKE_IMAGE")
    note.video_url = "https://example.invalid/v?xsec_token=FAKE_VIDEO"
    data = note.to_dict()
    assert "FAKE_" not in json.dumps(data)
    assert "width=320" in data["images"]
    assert "FAKE_" not in json.dumps(note.fact.__dict__, default=str)
    output = CsvExporter(str(tmp_path / "notes.csv")).export([note])
    assert "FAKE_" not in open(output, encoding="utf-8-sig").read()


@pytest.mark.parametrize("raw", [
    'xsec_token=FAKE%2FSECRET',
    'Authorization: Basic FAKE_SECRET',
    '{"Cookie": "FAKE_ONE; FAKE_TWO"}',
    "Cookie=FAKE_ONE,FAKE_TWO",
    'api_key="FAKE_SECRET"',
    'password: FAKE_SECRET',
])
def test_error_redaction_covers_complete_secret_values(raw):
    assert "FAKE" not in MediaCrawlerDriver()._sanitize_error_msg(raw)


@pytest.mark.parametrize("value", ["\n=1+1", "  =1+1", "\t @SUM(1,1)"])
def test_csv_whitespace_formula_prefix_is_escaped(value):
    assert sanitize_csv_cell(value).startswith("'")
