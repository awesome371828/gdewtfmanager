#!/bin/sh
# Restore verification into a dedicated EMPTY test database; never point this
# at the production database. This is a destructive restore of the target DB.
set -eu
: "${BACKUP_FILE:?BACKUP_FILE is required}"
: "${RESTORE_TEST_DATABASE_URL:?Use a separate disposable empty PostgreSQL database}"
command -v pg_restore >/dev/null 2>&1 || { echo "pg_restore is required" >&2; exit 2; }
test -s "$BACKUP_FILE"
pg_restore --exit-on-error --no-owner --no-acl --clean --if-exists \
  --dbname="$RESTORE_TEST_DATABASE_URL" "$BACKUP_FILE"
psql "$RESTORE_TEST_DATABASE_URL" -v ON_ERROR_STOP=1 -Atqc 'SELECT 1' | grep -qx 1
printf 'Restore completed and PostgreSQL query verified in isolated test database.\n'
