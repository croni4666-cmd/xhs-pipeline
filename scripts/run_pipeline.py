# -*- coding: utf-8 -*-
"""
xhs-pipeline CLI Runner (v0.2.0-aligned)
Unified command-line interface for AI agents and human researchers.

Features:
- Structured exit codes:
  * 0: Success.
  * 1: Argument / Partial success / Budget exhausted.
  * 2: Driver / Authentication / Network failure.
- Failure recovery (--resume-from): re-run downstream export without browser scrape.
- Resilience governance (--max-budget, --fallback-driver).
- Machine-readable structured JSON output (--format json).
- Safe parameter boundaries.
"""

import sys
import os
import json
import argparse
import asyncio
from pathlib import Path

# Add project root to sys.path
root_dir = str(Path(__file__).resolve().parent.parent)
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from core.contracts import CrawlRequest, DriverError, BudgetExceededError, CircuitBreakerOpenError
from core.pipeline import XhsPipeline
from scripts.analyze_insights import extract_insights_for_note


async def main():
    parser = argparse.ArgumentParser(description="Xiaohongshu Research & Scraping Pipeline")
    parser.add_argument("--keywords", "-k", type=str, default="", help="Comma-separated keywords")
    parser.add_argument("--limit", "-l", type=int, default=20, help="Max notes per keyword")
    parser.add_argument("--driver", "-d", type=str, default="mediacrawler", help="Scraper driver (mediacrawler | http | mock)")
    parser.add_argument("--fallback-driver", type=str, default="", help="Fallback driver if primary circuit breaker trips (default: none)")
    parser.add_argument("--max-budget", type=int, default=100, help="Maximum request budget")
    parser.add_argument("--rate-limit", type=int, default=30, help="Rate limit per minute")
    parser.add_argument("--resume-from", "-r", type=str, default="", help="Resume downstream processing from raw JSONL file")
    parser.add_argument("--comments", action="store_true", help="Enable comment scraping")
    parser.add_argument("--analyze", action="store_true", help="Run empirical pattern and insight extraction")
    parser.add_argument("--export-csv", type=str, default="", help="Export CSV path")
    parser.add_argument("--export-obsidian", type=str, default="", help="Export Obsidian cards directory")
    parser.add_argument("--state-file", type=str, default="", help="Path to save pipeline execution state JSON")
    parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format (text | json)")
    parser.add_argument("--health-check", action="store_true", help="Run driver health check and exit")

    args = parser.parse_args()
    pipeline = XhsPipeline()

    # Configure budget and rate limit from CLI args
    pipeline.budget_manager.max_budget = args.max_budget
    pipeline.budget_manager.rate_limit_per_min = args.rate_limit

    # 1. Health check mode
    if args.health_check:
        try:
            driver = pipeline.get_driver(args.driver)
            healthy = await driver.health_check()
            caps = driver.capabilities
            if args.format == "json":
                print(json.dumps({
                    "driver": driver.name,
                    "healthy": healthy,
                    "capabilities": {
                        "can_search": caps.can_search,
                        "can_get_detail": caps.can_get_detail,
                        "can_get_comments": caps.can_get_comments,
                        "requires_browser": caps.requires_browser
                    }
                }, indent=2))
            else:
                print(f"[*] Checking health of driver '{driver.name}'...")
                print(f"[*] Driver '{driver.name}' status: {'ONLINE' if healthy else 'OFFLINE'}")
                print(f"[*] Capabilities: search={caps.can_search}, detail={caps.can_get_detail}, comments={caps.can_get_comments}, browser={caps.requires_browser}")
            sys.exit(0 if healthy else 2)
        except Exception as e:
            if args.format == "json":
                print(json.dumps({"driver": args.driver, "healthy": False, "error": str(e)}))
            else:
                print(f"[x] Health check error: {e}")
            sys.exit(2)

    notes = []
    crawl_errors = []
    is_partial = False

    # 2. Failure Recovery Mode (Resume from file)
    if args.resume_from:
        if args.format != "json":
            print(f"[*] Resuming from existing cache: {args.resume_from}")
        try:
            notes = pipeline.load_from_jsonl(args.resume_from)
            if args.format != "json":
                print(f"[√] Loaded {len(notes)} cached notes successfully.")
        except Exception as e:
            if args.format == "json":
                print(json.dumps({"error": f"Failed loading cache: {str(e)}"}))
            else:
                print(f"[x] Failed loading cache: {e}")
            sys.exit(1)

    # 3. Live Crawl Mode
    elif args.keywords:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
        request = CrawlRequest(
            keywords=keywords,
            max_count_per_keyword=args.limit,
            enable_comments=args.comments
        )
        if args.format != "json":
            print(f"[*] Starting crawl via driver '{args.driver}' (fallback: '{args.fallback_driver}')...")
            print(f"[*] Keywords: {keywords} | Limit: {args.limit} | Budget: {args.max_budget}")

        try:
            response = await pipeline.execute_crawl(
                request,
                driver_name=args.driver,
                fallback_driver=args.fallback_driver
            )
            notes = response.notes
            crawl_errors = response.errors
            is_partial = response.is_partial
            if args.format != "json":
                print(f"[√] Crawl completed! Total notes fetched: {response.total_notes}")
                if crawl_errors:
                    print(f"[!] Warnings: {'; '.join(crawl_errors)}")
        except BudgetExceededError as e:
            if args.format == "json":
                print(json.dumps({"error": "BudgetExceededError", "message": str(e)}))
            else:
                print(f"[!] Budget exceeded: {e}")
            sys.exit(1)
        except CircuitBreakerOpenError as e:
            if args.format == "json":
                print(json.dumps({"error": "CircuitBreakerOpenError", "message": str(e)}))
            else:
                print(f"[x] Circuit breaker open: {e}")
            sys.exit(2)
        except DriverError as e:
            if args.format == "json":
                print(json.dumps({"error": "DriverError", "message": str(e)}))
            else:
                print(f"[x] Driver error: {e}")
            sys.exit(2)
        except Exception as e:
            if args.format == "json":
                print(json.dumps({"error": "UnexpectedError", "message": str(e)}))
            else:
                print(f"[x] Pipeline unexpected error: {e}")
            sys.exit(2)
    else:
        if args.format == "json":
            print(json.dumps({"error": "Either --keywords or --resume-from is required"}))
        else:
            print("[!] Error: Either --keywords or --resume-from is required.")
        sys.exit(1)

    # 4. Optional Insight Analysis
    if args.analyze and notes:
        for n in notes:
            ins = extract_insights_for_note(n)
            n.add_insight(ins)
        if args.format != "json":
            print(f"[√] Attached empirical insights to {len(notes)} notes.")

    # 5. Downstream Exports
    created_csv = ""
    created_cards = []
    if args.export_csv:
        created_csv = pipeline.export_csv(notes, args.export_csv)
        if args.format != "json":
            print(f"[√] Exported CSV to: {created_csv}")

    if args.export_obsidian:
        created_cards = pipeline.export_obsidian_cards(notes, args.export_obsidian)
        if args.format != "json":
            print(f"[√] Generated/Updated {len(created_cards)} idempotent Obsidian cards in: {args.export_obsidian}")

    # 6. Save Stage State if requested
    if args.state_file:
        pipeline.save_state(notes, args.state_file)
        if args.format != "json":
            print(f"[√] Pipeline stage state saved to: {args.state_file}")

    # Output structured JSON if requested
    if args.format == "json":
        result = {
            "success": len(notes) > 0,
            "total_notes": len(notes),
            "is_partial": is_partial,
            "errors": crawl_errors,
            "csv_path": created_csv,
            "obsidian_cards_count": len(created_cards),
            "budget_stats": pipeline.budget_manager.get_stats()
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("[*] Pipeline finished successfully.")

    if len(notes) == 0 and crawl_errors:
        sys.exit(2)
    elif is_partial:
        sys.exit(1)
    else:
        sys.exit(0)


def cli_entrypoint():
    """Synchronous entrypoint wrapper for setuptools console scripts."""
    try:
        asyncio.run(main())
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        sys.stderr.write(f"Fatal error: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    cli_entrypoint()
