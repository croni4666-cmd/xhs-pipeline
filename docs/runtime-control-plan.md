# Runtime control implementation plan

> Native execution through superpowers:executing-plans; tests precede product code. User authorized operational guards and confirmed framework-neutral interfaces. The current parent tracker is authoritative for live status.

Goal: add replaceable-driver runtime guards without changing content, snapshot or export contracts.
Spec: runtime-control-design.md. Python>=3.10, standard library control/storage; no new crawler dependency, no platform/account tests.

## Review focus

- Restart or another driver must not clear a risk pause or reset request history.
- A zero exit status with restriction logs must stop; a silent child is not session-ready.
- Network errors must not execute login or be mislabelled authentication expiry.
- Foreign browser contexts/pages and unrelated user work must survive cleanup.
- Unsupported integrations, corrupt state, malformed events and concurrent budget reservations must fail closed without exposing secrets.

## Tasks

1. Persistent control: create core/control/{contracts,store,runtime}.py. Interfaces SessionState, RuntimeCapabilities, RequestPolicy, ControlPausedError, ControlStore, RuntimeContext; prepare/ready/pause/before_request/report/lease. Write failing tests for durable pause, acknowledgment, request quotas, concurrency and corruption, then implement and commit.
2. Driver injection: add base prepare_run/control capabilities; pipeline context/lease and manifest budget_stats control report, CLI status/acknowledgment. Existing test-only legacy stubs explicitly opt out. Test unsupported refusal, alternate-driver pause reuse, no fallback/retry on restrictions, preserved manifest and offline operation, then implement and commit.
3. MediaCrawler bridge: new module runner, interface verification, patched automatic login/browser launch, session probe, HTTP/navigation hooks and owned-page cleanup; stream both output pipes into adapter-local normalized events. Test fake provider plus local child restriction, timeout/cancellation and malformed output; implement and commit.
4. Delivery: update packaged/source settings together, usage/extension docs and installed wheel smoke. Full suite, compile, wheel, one independent final read-only review, confirm/fix findings, separate dependent draft PR and CI matrix. No merge/release.
