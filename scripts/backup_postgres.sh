#!/bin/sh
# PostgreSQL custom-format backup with integrity/list verification.
# Required: DATABASE_URL, BACKUP_DIR. Optional: BACKUP_KEEP_DAYS (default 14).
set -eu
: "${DATABASE_URL:?DATABASE_URL is required}"
: "${BACKUP_DIR:=/var/backups/gdewtf}"
: "${BACKUP_KEEP_DAYS:=14}"
command -v pg_dump >/dev/null 2>&1 || { echo "pg_dump is required" >&2; exit 2; }
command -v pg_restore >/dev/null 2>&1 || { echo "pg_restore is required" >&2; exit 2; }
mkdir -p "$BACKUP_DIR"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
tmp="$BACKUP_DIR/gdewtf-$stamp.dump.tmp"
final="$BACKUP_DIR/gdewtf-$stamp.dump"
trap 'rm -f "$tmp"' EXIT HUP INT TERM
pg_dump --format=custom --no-owner --no-acl --verbose --dbname="$DATABASE_URL" --file="$tmp"
test -s "$tmp"
pg_restore --list "$tmp" >/dev/null
mv "$tmp" "$final"
find "$BACKUP_DIR" -type f -name 'gdewtf-*.dump' -mtime +"$BACKUP_KEEP_DAYS" -delete
printf 'Backup created and archive index verified: %s\n' "$final"
