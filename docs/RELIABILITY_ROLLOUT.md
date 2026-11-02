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
  recovery, and bounded latency samples. tests/test_durable_runtime.py covers
  update middleware duplicate suppression, handler failure recording, and DB fail-open behavior.
- durable_runtime.py adds leased, PostgreSQL-backed Telegram update claims and
  one-shot completion/failure recording. The dispatcher update middleware is wired
  into iris.py. Apply the migration before relying on durable deduplication; if the
  database is unavailable, middleware deliberately falls back to at-least-once processing.
- The Docker build compiles the new module and runs the full unittest discovery.
- index.html receives a restrained, responsive visual refresh for the Mini App shell.
  This is not yet a complete redesign of every Telegram message and inline keyboard.

## Remaining implementation work — do not treat these as completed yet

| # | Improvement | Current state |
|---|---|---|
| 1 | Cross-container single-instance protection | Implemented in PostgreSQL client creation; must be verified on the actual Relax Dev database |
| 2 | Durable GDE.WTF mutation outbox | Table prepared and helper methods added, but _api mutation journaling is not wired; uncertain writes are not yet recorded automatically. No automatic POST retry is allowed |
| 3 | Automated backups and restore drills | Daily backup worker validates archives. Requires persistent BACKUP_DIR; optional restore drill requires a separate BACKUP_RESTORE_TEST_DATABASE_URL. Neither external storage nor real restore has been verified here |
| 4 | Confirmation for destructive actions | Implemented for high-impact GDE.WTF commands with one-shot 45-second user/chat-bound callback tokens; in-memory only, so pending prompts do not survive a restart. Requires a real bot test, including reply-to-message targeting |
| 5 | Concurrency-safe business updates | Requires an audit of each counter/upsert path and transaction-level tests |
| 6 | GDE.WTF API circuit breaker | Connected to `_api()`: threshold 4, 10-second recovery, one half-open probe, no automatic mutation retries; unit tests cover recovery and probe cancellation. Needs live fault-injection verification |
| 7 | End-to-end Telegram/API/database latency | GDE.WTF request latency/error window and separate read/write bulkheads are connected. Telegram handler/API timing and transport tuning remain incomplete; do not change Telegram timeouts without measurements |
| 8 | Automatic degradation alerts | Owner notification worker now sends degradation/recovery transitions with API P95 and pool snapshot; delivery and threshold behavior need live verification |
| 9 | Telegram update deduplication | PostgreSQL lease-based claim/finish middleware wired; apply migration first. Retention/archival worker still missing. DB outages intentionally degrade to at-least-once handling |
| 10 | Background task bulkheads | GDE.WTF API reads (32) and writes (8) are isolated by separate semaphores. Other background workers still need independent limits |
| 11 | Versioned migrations | Migration history foundation prepared; migration runner and preflight/rollback policy still need implementation |
| 12 | Integration and failure-injection tests | Circuit-breaker unit tests expanded; Telegram/GDE.WTF/PostgreSQL integration and failure-injection tests are still missing. Tests have not been executed in this environment |
| 13 | GDE.WTF API contract tests | Not implemented yet; requires verified API response samples/contracts |
| 14 | Retention and archival | Event table prepared; retention policy and bounded cleanup worker still need implementation |
| 15 | Load and recovery tests | Not implemented yet; requires a staging bot/token and isolated test database |

## Not complete in this branch\n\nThe operation helper is not yet connected to mutation requests. The backup worker exists, but persistent backup storage and an isolated restore-test database are not configured here. Full Telegram message/keyboard redesign, real PostgreSQL/API integration tests, and load/recovery tests still require a staging bot, isolated database, and backup destination. Do not interpret a successful Python compile as proof of live API compatibility.\n\n## Deployment safety

Do not deploy this branch to the live bot until the Docker build and tests pass in the user's build environment and
the advisory lock has been verified against the same PostgreSQL database used by the
Relax Dev instance. The SQL migration is additive, but should still be backed up
before it is applied. Never replay an uncertain mutating API request until the
upstream API's idempotency behavior has been confirmed.

No GitHub Actions workflow is used to run the bot. The bot remains a long-running
process on Relax Dev.