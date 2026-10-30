# -*- coding: utf-8 -*-
"""
Export Xiaohongshu JSONL/CSV to Obsidian Markdown Cards
Transforms scraped notes into modular PKM cards ready for Obsidian with non-destructive preservation.
"""

import sys
import json
import argparse
from pathlib import Path

# Add project root to sys.path
root_dir = str(Path(__file__).resolve().parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from core.pipeline import XhsPipeline
from core.security import redact_error


def convert_jsonl_to_obsidian(
    input_file: str,
    vault_dir: str,
    *,
    is_snapshot: bool = False,
    allow_synthetic: bool = False,
    settings_path=None,
    output_format: str = "text",
):
    pipeline = XhsPipeline(settings_path) if settings_path else XhsPipeline()
    task_id = "default_task"
    if is_snapshot:
        resp = pipeline.resume_task_snapshot(input_file, allow_synthetic=allow_synthetic)
        notes = resp.notes
        if resp.manifest:
            task_id = resp.manifest.task_id
    else:
        is_auto_snapshot = False
        try:
            with open(input_file, "r", encoding="utf-8") as stream:
                head = stream.read(4096).lstrip()
            is_auto_snapshot = head.startswith("{") and '"xhs_task_snapshot_v1"' in head
        except OSError:
            pass
        if is_auto_snapshot:
            resp = pipeline.resume_task_snapshot(input_file, allow_synthetic=allow_synthetic)
            notes = resp.notes
            if resp.manifest:
                task_id = resp.manifest.task_id
        else:
            notes = pipeline.load_from_jsonl(input_file, allow_synthetic=allow_synthetic)
    created = pipeline.export_obsidian_cards(notes, vault_dir, task_id=task_id)
    if output_format == "json":
        print(json.dumps({
            "success": True,
            "obsidian_cards_count": len(created),
            "vault_dir": vault_dir,
            "task_id": task_id,
        }, ensure_ascii=False, indent=2))
    else:
        print(f"[√] Successfully exported/updated {len(created)} idempotent Obsidian cards in: {vault_dir}")
    return created


if __name__ == "__main__":
    for _stream in (sys.stdout, sys.stderr):
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser(description="Export JSONL or Task Snapshot to Obsidian Cards")
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument("--input", "-i", type=str, default="", help="Path to scraped JSONL or snapshot file")
    inputs.add_argument("--snapshot", type=str, default="", help="Path to versioned task snapshot JSON")
    parser.add_argument("--output", "-o", type=str, default="./obsidian_cards", help="Obsidian target folder")
    parser.add_argument("--allow-synthetic", action="store_true", help="Explicitly permit labeled synthetic data")
    parser.add_argument("--settings", default=None, help="Local settings JSON")
    parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format (text or json)")
    args = parser.parse_args()

    try:
        convert_jsonl_to_obsidian(
            args.snapshot or args.input,
            args.output,
            is_snapshot=bool(args.snapshot),
            allow_synthetic=args.allow_synthetic,
            settings_path=args.settings,
            output_format=args.format,
        )
    except Exception as exc:
        msg = redact_error(exc)
        if args.format == "json":
            print(json.dumps({"success": False, "error": type(exc).__name__, "message": msg}, ensure_ascii=False))
        else:
            sys.stderr.write(f"[x] Export failed: {msg}\n")
        sys.exit(1)
