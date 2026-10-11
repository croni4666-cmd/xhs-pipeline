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

from core.contracts import CrawlRequest, CrawlResponse, DriverError, BudgetExceededError, CircuitBreakerOpenError, StageStatus
from core.pipeline import XhsPipeline
from core.security import redact_error
from scripts.analyze_insights import extract_insights_for_note


async def main():
    parser = argparse.ArgumentParser(description="Xiaohongshu Research & Scraping Pipeline")
    inputs = parser.add_mutually_exclusive_group()
    inputs.add_argument("--keywords", "-k", type=str, default="", help="Comma-separated keywords")
    parser.add_argument("--limit", "-l", type=int, default=20, help="Max notes per keyword")
    parser.add_argument("--driver", "-d", type=str, default="mediacrawler", help="Scraper driver (mediacrawler | http | mock)")
    parser.add_argument("--fallback-driver", type=str, default="", help="Fallback driver if primary circuit breaker trips (default: none)")
    parser.add_argument("--max-budget", type=int, default=100, help="Maximum driver attempts, including retries (not internal HTTP requests)")
    parser.add_argument("--rate-limit", type=int, default=30, help="Driver attempts per minute")
    parser.add_argument("--allow-synthetic", action="store_true", help="Explicitly permit labeled mock/HTTP demonstration data")
    parser.add_argument("--settings", default=None, help="Local settings JSON")
    parser.add_argument("--sort", default="popularity_descending", choices=["general", "popularity_descending", "latest", "time_descending"])
    parser.add_argument("--media", action="store_true", help="Enable media download in the external driver")
    inputs.add_argument("--resume-from", "-r", type=str, default="", help="Resume downstream processing from raw JSONL file")
    inputs.add_argument("--resume-state", type=str, default="", help="Recover a complete task snapshot offline")
    inputs.add_argument('--control-status', action='store_true', help='Read local online-control state without platform access')
    inputs.add_argument('--acknowledge-pause', action='store_true', help='Acknowledge local pause; next online task must verify its session again')
    parser.add_argument('--control-scope', default=None, help='Nonsecret persistent session scope shared across crawler frameworks')
    parser.add_argument("--comments", action="store_true", help="Enable comment scraping")
    parser.add_argument("--analyze", action="store_true", help="Run empirical pattern and insight extraction")
    parser.add_argument("--export-csv", type=str, default="", help="Export CSV path")
    parser.add_argument("--export-obsidian", type=str, default="", help="Export Obsidian cards directory")
    parser.add_argument("--state-file", type=str, default="", help="Path to save pipeline execution state JSON")
    parser.add_argument("--snapshot-file", type=str, default="", help="Save a complete versioned task snapshot without access credentials")
    parser.add_argument("--format", "-f", choices=["text", "json"], default="text", help="Output format (text | json)")
    parser.add_argument("--health-check", action="store_true", help="Run driver health check and exit")

    args = parser.parse_args()
    if args.limit < 1 or args.max_budget < 0 or args.rate_limit < 1:
        parser.error("limit/rate-limit must be positive and max-budget nonnegative")
    if args.health_check and (args.keywords or args.resume_from or args.resume_state or args.control_status or args.acknowledge_pause):
        parser.error("health-check cannot be combined with a capture or recovery input")
    pipeline = XhsPipeline(args.settings) if args.settings else XhsPipeline()
    if args.allow_synthetic and hasattr(pipeline, "settings"):
        pipeline.settings["allow_synthetic"] = True
    if args.control_scope is not None:
        pipeline.settings['control_scope'] = args.control_scope
    if args.control_status or args.acknowledge_pause:
        runtime = pipeline.create_runtime(CrawlRequest([]))
        if args.acknowledge_pause:
            pipeline.control_store.acknowledge(runtime.scope)
        report = runtime.report()
        print(json.dumps({'success': True, 'control': report}, ensure_ascii=False) if args.format == 'json' else str(report))
        return

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
                print(json.dumps({"driver": args.driver, "healthy": False, "error": redact_error(e)}))
            else:
                print(f"[x] Health check error: {redact_error(e)}")
            sys.exit(2)

    notes = []
    crawl_errors = []
    is_partial = False
    crawl_success = True
    response = None

    # 2. Failure Recovery Mode (Resume from file)
    if args.resume_state or args.resume_from:
        if args.format != "json":
            print(f"[*] Restoring offline input: {args.resume_state or args.resume_from}")
        try:
            if args.resume_state:
                response = pipeline.resume_task_snapshot(args.resume_state, allow_synthetic=args.allow_synthetic)
            else:
                response = pipeline.import_jsonl_task(args.resume_from, allow_synthetic=args.allow_synthetic)
            notes = response.notes
            crawl_success, is_partial = response.success, response.is_partial
            crawl_errors = [redact_error(error) for error in response.errors]
            if args.format != "json":
                print(f"[√] Loaded {len(notes)} cached notes successfully.")
        except Exception as e:
            if args.format == "json":
                print(json.dumps({"success": False, "error": f"Failed restoring offline input: {redact_error(e)}"}))
            else:
                print(f"[x] Failed loading cache: {redact_error(e)}")
            sys.exit(1)

    # 3. Live Crawl Mode
    elif args.keywords:
        keywords = [k.strip() for k in args.keywords.split(",") if k.strip()]
        request = CrawlRequest(
            keywords=keywords,
            max_count_per_keyword=args.limit,
            enable_comments=args.comments, enable_media=args.media,
            sort_type=args.sort, allow_synthetic=args.allow_synthetic
        )
        if args.format != "json":
            print(f"[*] Starting crawl via driver '{args.driver}' (fallback: '{args.fallback_driver}')...")
            print(f"[*] Keywords: {keywords} | Limit: {args.limit} | Budget: {args.max_budget}")

        def fail_capture(error, name, code):
            result = {'success': False, 'error': name, 'message': redact_error(error)}
            if args.snapshot_file:
                try:
                    manifest = pipeline.checkpoint_store.load_task_manifest(request.task_id)
                    if manifest is None:
                        raise RuntimeError('The failed task has no recoverable manifest')
                    failed = CrawlResponse(False, manifest.driver_name, 0, [], errors=list(manifest.errors),
                                           manifest=manifest, is_synthetic=manifest.is_synthetic,
                                           data_source=manifest.data_source,
                                           detail_success_count=manifest.detail_success_count,
                                           detail_failed_count=manifest.detail_failed_count,
                                           actual_request_count=manifest.actual_request_count)
                    pipeline.save_task_snapshot(failed, args.snapshot_file)
                    result['snapshot_path'] = args.snapshot_file
                except Exception as snapshot_error:
                    result['snapshot_error'] = redact_error(snapshot_error)
            if args.format == 'json':
                print(json.dumps(result, ensure_ascii=False))
            else:
                print(f"[x] {name}: {result['message']}")
                if 'snapshot_error' in result:
                    print(f"[!] Snapshot could not be saved: {result['snapshot_error']}")
            sys.exit(code)

        try:
            response = await pipeline.execute_crawl(
                request,
                driver_name=args.driver,
                fallback_driver=args.fallback_driver
            )
            notes = response.notes
            crawl_errors = [redact_error(error) for error in response.errors]
            is_partial = response.is_partial
            crawl_success = response.success
            if args.format != "json":
                print(f"[√] Crawl completed! Total notes fetched: {response.total_notes}")
                if crawl_errors:
                    print(f"[!] Warnings: {'; '.join(crawl_errors)}")
        except BudgetExceededError as e:
            fail_capture(e, 'BudgetExceededError', 1)
        except CircuitBreakerOpenError as e:
            fail_capture(e, 'CircuitBreakerOpenError', 2)
        except DriverError as e:
            fail_capture(e, 'DriverError', 2)
        except Exception as e:
            fail_capture(e, 'UnexpectedError', 2)
    else:
        if args.format == "json":
            print(json.dumps({"error": "One of --keywords, --resume-from or --resume-state is required"}))
        else:
            print("[!] Error: One of --keywords, --resume-from or --resume-state is required.")
        sys.exit(1)

    # 4. Optional Insight Analysis
    if args.analyze and notes:
        for n in notes:
            ins = extract_insights_for_note(n)
            n.add_insight(ins)
            n.stage_status = StageStatus.ANALYZED
        if args.format != "json":
            print(f"[√] Attached empirical insights to {len(notes)} notes.")

    # 5. Downstream Exports
    if args.snapshot_file:
        # Keep recoverable inputs even if a later export encounters a conflict.
        pipeline.save_task_snapshot(response, args.snapshot_file)
    created_csv = ""
    created_cards = []
    if args.export_csv:
        created_csv = pipeline.export_csv(notes, args.export_csv)
        if args.format != "json":
            print(f"[√] Exported CSV to: {created_csv}")

    if args.export_obsidian:
        export_task = response.manifest.task_id if getattr(response, "manifest", None) else "cache_export"
        created_cards = pipeline.export_obsidian_cards(notes, args.export_obsidian, task_id=export_task)
        if args.format != "json":
            print(f"[√] Generated/Updated {len(created_cards)} idempotent Obsidian cards in: {args.export_obsidian}")

    # 6. Save Stage State if requested
    if args.state_file:
        pipeline.save_state(notes, args.state_file)
        if args.format != "json":
            print(f"[√] Pipeline stage state saved to: {args.state_file}")

    # Determine overall execution outcome strictly based on crawl_success
    overall_success = bool(crawl_success and not (len(notes) == 0 and (crawl_errors or not crawl_success)))
    if not crawl_success:
        overall_success = False

    # Output structured JSON if requested
    if args.format == "json":
        result = {
            "success": overall_success,
            "total_notes": len(notes),
            "is_partial": is_partial,
            "is_synthetic": bool(getattr(response, 'is_synthetic', False) or any(note.is_synthetic for note in notes)),
            "data_sources": sorted({note.data_source for note in notes}),
            "errors": crawl_errors,
            "csv_path": created_csv,
            "obsidian_cards_count": len(created_cards),
            "snapshot_path": args.snapshot_file,
            "operation": getattr(getattr(response, 'manifest', None), 'operation', 'crawl'),
            "task_id": getattr(getattr(response, 'manifest', None), 'task_id', None),
            "parent_task_id": getattr(getattr(response, 'manifest', None), 'parent_task_id', None),
            "budget_stats": pipeline.budget_manager.get_stats()
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if overall_success:
            print("[*] Pipeline finished successfully.")
        else:
            print("[!] Pipeline completed with errors or failure.")

    if not overall_success:
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
        message = redact_error(e)
        if "json" in sys.argv and ("--format" in sys.argv or "-f" in sys.argv):
            print(json.dumps({"success": False, "error": type(e).__name__, "message": message}, ensure_ascii=False))
        else:
            sys.stderr.write(f"Fatal error: {message}\n")
        sys.exit(1)


if __name__ == "__main__":
    cli_entrypoint()
