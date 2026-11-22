# Operations: backups, restore verification, and reliability checks

## PostgreSQL backups (Relax Dev)

The bot cannot create a backup of the managed PostgreSQL service unless the host
provides a scheduled job/container with PostgreSQL client tools and the same
DATABASE_URL. Keep backups outside the database service and outside the bot
container's ephemeral filesystem.

Run `scripts/backup_postgres.sh` from a scheduled Relax Dev task or an external
trusted backup runner. Set DATABASE_URL, BACKUP_DIR on persistent storage, and
BACKUP_KEEP_DAYS. Schedule daily (for example, 02:30 UTC) using the host's
scheduler; GitHub Actions are intentionally not used.

The script writes a custom-format pg_dump archive, verifies its archive index
with pg_restore --list, atomically renames the completed file, and prunes old
archives. This is an archive-integrity check, not proof of a successful restore.

## Restore drill

Provision a separate, disposable, empty PostgreSQL database. Set
BACKUP_FILE and RESTORE_TEST_DATABASE_URL and run
`scripts/verify_restore.sh`. The script destructively restores into that
database and verifies a SQL query. Never set RESTORE_TEST_DATABASE_URL to the
production DATABASE_URL. Repeat this drill periodically and retain its logs.

## Tests

- Build gate: `python -m unittest discover -s tests -v`
- Local API concurrency smoke test: `python -m unittest discover -s tests -p 'test_api_load.py' -v`
- PostgreSQL integration test: set TEST_DATABASE_URL to a disposable database,
  then run `python -m unittest discover -s tests -p 'test_postgres_integration.py' -v`.

The API load test targets a local stub only; it does not issue live moderation
commands. The PostgreSQL test is skipped unless an isolated test DSN is set.

## Delivery semantics and ambiguous GDE.WTF writes

Telegram update IDs are persisted before handler execution. Completed and
interrupted updates are not replayed blindly. An interrupted operation can have
reached the remote service even if the response was lost.

The existing bot_gdewtf_sync_outbox table is an intent journal, not a remote
idempotency guarantee. GDE.WTF's documented API does not expose a confirmed
idempotency-key parameter. Therefore a timed-out POST must be recorded as
uncertain and reconciled against an authoritative remote read before any
operator-approved retry. Do not auto-replay warn/ban/unban/remove requests.

## Latency profiling

The bot logs slow Telegram updates, Telegram Bot API calls, GDE.WTF HTTP calls,
and PostgreSQL query spans. Compare p50/p95/max under representative real
traffic; do not use synthetic local timings as a production SLA.
