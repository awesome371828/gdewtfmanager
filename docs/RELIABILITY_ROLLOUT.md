# GDE.WTF reliability and visual refresh rollout

This branch is a staged reliability upgrade. It is intentionally additive: existing
commands, moderation handlers, settings, and stored records are not removed.

## Changes in this branch

- reliability.py adds a dependency-light async circuit breaker with a single
  half-open probe and a bounded rolling latency/error window.
- postgres_adapter.py adds a dedicated PostgreSQL session advisory lock.
  create_client_from_env() acquires the GDE.WTF lock by default; a conflict
  exits with code 23 so the existing entrypoint does not create a restart loop.
- postgres_adapter.py exposes the connection-pool counters for diagnostics.
- migrations/20261102_001_reliability_foundation.sql adds additive tables for
  an operation outbox, Telegram update deduplication, reliability events, and migration
  history. The migration is packaged in the image but is not auto-applied.
- tests/test_reliability.py covers circuit opening, half-open probe exclusivity,
  recovery, and bounded latency samples.
- The Docker build compiles the new module and runs the full unittest discovery.
- index.html receives a restrained, responsive visual refresh: muted forest
  palette, clearer hierarchy, accessible tabs, and less decorative UI.

## Remaining implementation work — do not treat these as completed yet

| # | Improvement | Current state |
|---|---|---|
| 1 | Cross-container single-instance protection | Implemented in PostgreSQL client creation; must be verified on the actual Relax Dev database |
| 2 | Durable GDE.WTF mutation outbox | Tables prepared; mutation handlers still need operation-by-operation integration and upstream idempotency confirmation |
| 3 | Automated backups and restore drills | Not configured by this code change; must be configured for the Relax Dev database and tested |
| 4 | Confirmation for destructive bulk actions | Not implemented yet |
| 5 | Concurrency-safe business updates | Requires an audit of each counter/upsert path and transaction-level tests |
| 6 | GDE.WTF API circuit breaker | Primitive and tests added; still needs wiring into the existing _api() transport function |
| 7 | End-to-end Telegram/API/database latency | Pool snapshot is available; handler-level timing, Telegram transport tuning, and owner diagnostics still need integration |
| 8 | Automatic degradation alerts | Rolling metric primitive added; alert thresholds and owner notification worker still need integration |
| 9 | Telegram update deduplication | Table prepared; dispatcher middleware and retention policy still need implementation |
| 10 | Background task bulkheads | Separate concurrency limits still need to be connected to the relevant workers |
| 11 | Versioned migrations | Migration history foundation prepared; migration runner and preflight/rollback policy still need implementation |
| 12 | Integration and failure-injection tests | Primitive tests added; Telegram/GDE.WTF/PostgreSQL scenario tests still need implementation |
| 13 | GDE.WTF API contract tests | Not implemented yet; requires verified API response samples/contracts |
| 14 | Retention and archival | Event table prepared; retention policy and bounded cleanup worker still need implementation |
| 15 | Load and recovery tests | Not implemented yet; requires a staging bot/token and isolated test database |

## Deployment safety

Do not deploy this branch to the live bot until the Docker build and tests pass and
the advisory lock has been verified against the same PostgreSQL database used by the
Relax Dev instance. The SQL migration is additive, but should still be backed up
before it is applied. Never replay an uncertain mutating API request until the
upstream API's idempotency behavior has been confirmed.

No GitHub Actions workflow is used to run the bot. The bot remains a long-running
process on Relax Dev.