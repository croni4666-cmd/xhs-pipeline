# -*- coding: utf-8 -*-
"""
Export Xiaohongshu JSONL/CSV to Obsidian Markdown Cards
Transforms scraped notes into modular PKM cards ready for Obsidian with non-destructive preservation.
"""

import sys
import argparse
from pathlib import Path

# Add project root to sys.path
root_dir = str(Path(__file__).resolve().parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from core.pipeline import XhsPipeline


def convert_jsonl_to_obsidian(jsonl_file: str, vault_dir: str):
    pipeline = XhsPipeline()
    notes = pipeline.load_from_jsonl(jsonl_file)
    created = pipeline.export_obsidian_cards(notes, vault_dir)
    print(f"[√] Successfully exported/updated {len(created)} idempotent Obsidian cards in: {vault_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export JSONL to Obsidian Cards")
    parser.add_argument("--input", "-i", type=str, required=True, help="Path to scraped JSONL file")
    parser.add_argument("--output", "-o", type=str, default="./obsidian_cards", help="Obsidian target folder")
    args = parser.parse_args()

    convert_jsonl_to_obsidian(args.input, args.output)
