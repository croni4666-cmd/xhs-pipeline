# -*- coding: utf-8 -*-
"""
MediaCrawler Driver Implementation (v0.3.0)
Adapts the local NanmiCoder/MediaCrawler repository to the BaseCrawlerDriver contract.

Improvements:
- Clean decoupling of NoteReference (encapsulates xsec_token and access context).
- Graceful cancellation handling (terminates background subprocess upon asyncio.CancelledError).
- Robust error classification (AuthenticationError, RateLimitError, ReferenceExpiredError).
- Evaluates ContentCompleteness and FieldPresence.
"""

import os
import sys
import json
import time
import ast
import hashlib
import asyncio
import re
import logging
import urllib.request
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
from ..security import redact_error
from ..persistence import atomic_write_bytes

from .base import BaseCrawlerDriver
from ..contracts import (
    CrawlRequest,
    CrawlResponse,
    UnifiedNote,
    NoteReference,
    EngagementMetrics,
    DiscoveryRecord,
    ContentCompleteness,
    FieldPresence,
    DriverCapabilities,
    DriverError,
    AuthenticationError,
    RateLimitError,
    ReferenceExpiredError,
    sanitize_note_url
)

logger = logging.getLogger("xhs_pipeline.drivers.mediacrawler")


class MediaCrawlerDriver(BaseCrawlerDriver):
    """Driver wrapping local MediaCrawler via Chrome DevTools Protocol (CDP)."""

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        self.cfg = config_dict or {}
        self.mediacrawler_path = self.cfg.get(
            "mediacrawler_path",
            os.environ.get("MEDIACRAWLER_PATH", "./MediaCrawler")
        )
        self.python_path = self.cfg.get(
            "python_path",
            os.environ.get("PYTHON_EXECUTABLE", sys.executable or "python")
        )
        self.cdp_port = self.cfg.get("cdp_port", 9222)
        self._current_subprocess: Optional[asyncio.subprocess.Process] = None
        self._runtime = None

    @property
    def runtime_capabilities(self):
        from ..control import RuntimeCapabilities
        return RuntimeCapabilities(True, True, True)

    async def prepare_run(self, runtime):
        self._runtime = runtime

    def _observe_control_line(self, line, channel):
        from ..control import SessionState
        if line.startswith('XHS_CONTROL_EVENT '):
            try:
                event = json.loads(line[len('XHS_CONTROL_EVENT '):])
                if event.get('schema') != 'runtime_event_v1': raise ValueError()
                if set(event) == {'schema', 'state'}:
                    state = SessionState(event['state'])
                    if state in (SessionState.AUTH_REQUIRED, SessionState.CHALLENGE, SessionState.RATE_LIMITED, SessionState.ACCESS_RESTRICTED):
                        self._runtime.pause(state)
                    self._runtime.observe(state)
                elif event == {'schema': 'runtime_event_v1', 'stop': 'budget_exceeded'}:
                    from ..contracts import BudgetExceededError
                    raise BudgetExceededError('Controlled request budget exhausted')
                else: raise ValueError()
            except (ValueError, TypeError, AttributeError):
                raise DriverError('Malformed runtime control event') from None
        elif re.search(r'(?i)(?:^|\]\s*)(?:Access restricted|XHS request blocked|CAPTCHA appeared|XHS account security restriction)', line):
            if re.search(r'(?i)captcha|461|471', line): state = SessionState.CHALLENGE
            elif '429' in line: state = SessionState.RATE_LIMITED
            elif '401' in line: state = SessionState.AUTH_REQUIRED
            else: state = SessionState.ACCESS_RESTRICTED
            self._runtime.pause(state)

    @property
    def name(self) -> str:
        return "mediacrawler"

    @property
    def capabilities(self) -> DriverCapabilities:
        return DriverCapabilities(
            can_search=True,
            can_get_detail=False,
            can_get_comments=True,
            can_get_media=True,
            requires_browser=True,
            supports_resume=True
        )

    async def initialize(self) -> None:
        if not os.path.isdir(self.mediacrawler_path):
            raise DriverError(f"MediaCrawler directory does not exist: {self.mediacrawler_path}")

    async def close(self):
        if self._current_subprocess is not None:
            await self._terminate(self._current_subprocess)
            self._current_subprocess = None

    async def health_check(self) -> bool:
        if not os.path.isdir(self.mediacrawler_path):
            return False

        try:
            req = urllib.request.Request(f"http://localhost:{self.cdp_port}/json/version")
            with urllib.request.urlopen(req, timeout=2) as resp:
                data = json.loads(resp.read().decode())
                return "webSocketDebuggerUrl" in data
        except Exception:
            return False

    def _sanitize_error_msg(self, msg: str) -> str:
        return redact_error(msg)

    def _configure_mediacrawler(self, request: CrawlRequest) -> None:
        config_file = os.path.join(self.mediacrawler_path, "config", "base_config.py")
        if not os.path.exists(config_file):
            raise DriverError(f"MediaCrawler config not found at: {config_file}")

        with open(config_file, "r", encoding="utf-8") as f:
            content = f.read()

        sort_type = {"latest": "time_descending"}.get(request.sort_type, request.sort_type)
        if sort_type not in ("general", "popularity_descending", "time_descending"):
            from ..contracts import CapabilityUnsupportedError
            raise CapabilityUnsupportedError("Unsupported Xiaohongshu sort mode")
        values = {"KEYWORDS": ",".join(request.keywords),
                  "CRAWLER_MAX_NOTES_COUNT": int(request.max_count_per_keyword),
                  "ENABLE_GET_COMMENTS": bool(request.enable_comments),
                  "ENABLE_GET_MEDIA": bool(request.enable_media), "CDP_DEBUG_PORT": int(self.cdp_port),
                  "ENABLE_CDP_MODE": True, "CDP_CONNECT_EXISTING": True,
                  "AUTO_CLOSE_BROWSER": False, "SORT_TYPE": sort_type,
                  "PLATFORM": "xhs", "CRAWLER_TYPE": "search", "SAVE_DATA_OPTION": "jsonl"}
        if self._runtime is not None:
            values.update(MAX_CONCURRENCY_NUM=1, SAVE_LOGIN_STATE=True)
        source = self._rewrite_assignments(content, values)
        platform_file = os.path.join(self.mediacrawler_path, "config", "xhs_config.py")
        platform_source = None
        if os.path.exists(platform_file):
            with open(platform_file, encoding="utf-8") as stream:
                platform_source = self._rewrite_assignments(stream.read(), {"SORT_TYPE": sort_type})
        if platform_source is not None:
            atomic_write_bytes(platform_file, platform_source)
        atomic_write_bytes(config_file, source)
        self.effective_config = {**values, "config_sha256": hashlib.sha256(source).hexdigest()}

    @staticmethod
    def _rewrite_assignments(content, values):

        try:
            tree = ast.parse(content)
            source = content.encode("utf-8")
            offsets = [0]
            for line in source.splitlines(keepends=True):
                offsets.append(offsets[-1] + len(line))
            replacements, seen, allowed_targets = [], set(), []
            for node in tree.body:
                targets = (node.targets if isinstance(node, ast.Assign)
                           else [node.target] if isinstance(node, ast.AnnAssign) else [])
                for target in targets:
                    if not isinstance(target, ast.Name) or target.id not in values:
                        continue
                    if target.id in seen or len(targets) != 1 or node.value is None:
                        raise ValueError("Ambiguous crawler configuration assignment")
                    seen.add(target.id)
                    allowed_targets.append(target)
                    value = values[target.id]
                    literal = json.dumps(value, ensure_ascii=False) if isinstance(value, str) else repr(value)
                    start = offsets[node.value.lineno - 1] + node.value.col_offset
                    end = offsets[node.value.end_lineno - 1] + node.value.end_col_offset
                    replacements.append((start, end, literal.encode("utf-8")))
            for node in ast.walk(tree):
                if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store) and node.id in values:
                    if node not in allowed_targets:
                        raise ValueError("Conditional crawler configuration is unsupported")
            for start, end, literal in sorted(replacements, reverse=True):
                source = source[:start] + literal + source[end:]
            for name in sorted(values.keys() - seen):
                value = values[name]
                literal = json.dumps(value, ensure_ascii=False) if isinstance(value, str) else repr(value)
                source += f"\n{name} = {literal}\n".encode("utf-8")
            updated = ast.parse(source)
            for node in updated.body:
                targets = (node.targets if isinstance(node, ast.Assign)
                           else [node.target] if isinstance(node, ast.AnnAssign) else [])
                for target in targets:
                    if isinstance(target, ast.Name) and target.id in values:
                        if ast.literal_eval(node.value) != values[target.id]:
                            raise ValueError("Crawler configuration verification failed")
        except (SyntaxError, ValueError, TypeError) as exc:
            raise DriverError(redact_error(f"Invalid crawler configuration: {exc}")) from exc
        return source

    def _determine_completeness(self, note_type: str, desc: str) -> ContentCompleteness:
        if not desc or len(desc.strip()) == 0:
            return ContentCompleteness.EMPTY
        if note_type == "video" and len(desc.strip()) < 40 and "#" in desc:
            return ContentCompleteness.VIDEO_ONLY
        return ContentCompleteness.FULL

    @property
    def version(self):
        import subprocess
        try:
            result = subprocess.run(["git", "-C", self.mediacrawler_path, "rev-parse", "HEAD"],
                                    capture_output=True, text=True, timeout=2, check=False)
            revision = result.stdout.strip()
            return revision if re.fullmatch(r"[0-9a-f]{40}", revision) else "unknown"
        except (OSError, subprocess.TimeoutExpired):
            return "unknown"

    async def _terminate(self, process):
        if process.returncode is not None:
            await process.wait()
            return
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.wait(), timeout=3)
        except asyncio.TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.wait()

    def _collect_new_records(self, data_dir: Path, before: Dict[str, Any], request: CrawlRequest):
        from ..normalizer import normalize_raw_note
        notes, references, errors, output_files = [], [], [], {}
        for file in sorted(data_dir.glob("search_contents_*.jsonl")):
            if not file.is_file() or file.is_symlink():
                continue
            data = file.read_bytes()
            snapshot = before.get(str(file))
            if snapshot and hashlib.sha256(data).hexdigest() == snapshot[1]:
                continue
            if snapshot and len(data) >= snapshot[0] and hashlib.sha256(data[:snapshot[0]]).hexdigest() == snapshot[1]:
                data = data[snapshot[0]:]
            if not data.strip():
                continue
            output_files["jsonl" if not output_files else f"jsonl_{len(output_files)}"] = str(file)
            for index, line in enumerate(data.decode("utf-8").splitlines(), 1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    keyword = raw.get("source_keyword")
                    if request.keywords and keyword not in request.keywords:
                        continue
                    note = normalize_raw_note(raw, keyword=keyword, rank=raw.get("rank"), data_source="mediacrawler")
                    notes.append(note)
                    references.append(NoteReference(
                        note_id=note.note_id, driver_name=self.name, title_hint=note.title,
                        author_hint=note.author_name, access_token=str(raw.get("xsec_token") or ""),
                        discovered_keyword=keyword or "", rank=raw.get("rank"), can_resume_cross_process=False))
                except (ValueError, TypeError, AttributeError):
                    errors.append(f"Malformed output record {index}")
        return notes, references, errors, output_files

    async def crawl_keywords(self, request):
        from ..persistence import async_file_lock
        from ..contracts import PipelineError
        if self._runtime is None and self.cfg.get('require_runtime_control', True) is not False:
            from ..contracts import CapabilityUnsupportedError
            raise CapabilityUnsupportedError('Live MediaCrawler requires a runtime control context')
        if not await self.health_check():
            return CrawlResponse(False, self.name, 0, [], errors=["Local CDP endpoint is unavailable"])
        async with async_file_lock(Path(self.mediacrawler_path) / ".xhs-pipeline.lock"):
            self._configure_mediacrawler(request)
            data_dir = Path(self.cfg.get("mediacrawler_output_dir", str(Path(self.mediacrawler_path) / "data" / "xhs" / "jsonl")))
            before = {}
            for file in data_dir.glob("search_contents_*.jsonl"):
                if file.is_file() and not file.is_symlink():
                    data = file.read_bytes()
                    before[str(file)] = (len(data), hashlib.sha256(data).hexdigest())
            env = os.environ.copy()
            env["PYTHONIOENCODING"] = "utf-8"
            arguments = ['main.py']
            input_payload = None
            if self._runtime is not None:
                expected = self.cfg.get('expected_mediacrawler_revision')
                if expected and self.version != expected:
                    raise DriverError('MediaCrawler revision does not match the configured pin')
                arguments = ['-m', 'core.drivers.mediacrawler_runtime']
                package_root = str(Path(__file__).resolve().parents[2])
                env['PYTHONPATH'] = package_root + os.pathsep + env.get('PYTHONPATH', '')
                payload = {'runtime': self._runtime.bridge_config(), 'note_limit': request.max_count_per_keyword}
                input_payload = (json.dumps(payload, ensure_ascii=False) + '\n').encode('utf-8')
            process = await asyncio.create_subprocess_exec(
                self.python_path, *arguments, cwd=self.mediacrawler_path,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE if input_payload is not None else None, env=env)
            self._current_subprocess = process
            try:
                timeout = float(self.cfg.get('crawl_timeout_seconds', 300))
                if self._runtime is not None:
                    from .process_monitor import monitor_process
                    stdout, stderr = await monitor_process(process, self._observe_control_line, self._terminate, timeout, input_payload)
                    if self._runtime.report()['state'] != 'ready':
                        raise DriverError('MediaCrawler did not confirm a valid existing session')
                else:
                    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
            except asyncio.CancelledError:
                await self._terminate(process)
                raise
            except asyncio.TimeoutError as exc:
                await self._terminate(process)
                err = DriverError("Crawler operation timed out")
                notes, references, errors, output_files = self._collect_new_records(data_dir, before, request)
                if notes or errors:
                    err.partial_response = CrawlResponse(
                        False, self.name, len(notes), notes, references=references,
                        errors=errors, output_files=output_files,
                        is_partial=bool(notes), data_source="mediacrawler")
                raise err from exc
            except PipelineError as exc:
                notes, references, errors, output_files = self._collect_new_records(data_dir, before, request)
                if notes or errors:
                    exc.partial_response = CrawlResponse(
                        False, self.name, len(notes), notes, references=references,
                        errors=errors, output_files=output_files,
                        is_partial=bool(notes), data_source="mediacrawler")
                raise
            finally:
                self._current_subprocess = None
            notes, references, errors, output_files = self._collect_new_records(data_dir, before, request)
            if process.returncode != 0:
                error = self._sanitize_error_msg(stderr.decode("utf-8", errors="replace"))
                return CrawlResponse(
                    False, self.name, len(notes), notes, references=references,
                    errors=[f"Crawler failed: {error[:300]}"] + errors, output_files=output_files,
                    is_partial=bool(notes), data_source="mediacrawler")
            return CrawlResponse(
                success=bool(notes) or not errors, driver_name=self.name, total_notes=len(notes),
                notes=notes, references=references, errors=errors, output_files=output_files,
                is_partial=bool(errors), data_source="mediacrawler",
                detail_success_count=None, detail_failed_count=None, actual_request_count=None)

    async def get_note_detail(self, target: Union[str, NoteReference]) -> Optional[UnifiedNote]:
        note_id = target.note_id if isinstance(target, NoteReference) else target
        if "xiaohongshu.com/explore/" in note_id:
            note_id = note_id.split("explore/")[1].split("?")[0]

        if isinstance(target, NoteReference) and target.is_expired():
            raise ReferenceExpiredError(f"Access token for note {note_id} has expired.", note_id=note_id)

        from ..contracts import CapabilityUnsupportedError
        raise CapabilityUnsupportedError("This adapter does not implement direct note detail queries")
