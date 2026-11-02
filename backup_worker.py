"""Scheduled PostgreSQL backups with archive validation and optional isolated restore drills."""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("pear_cm.backups")


async def _run(*args: str, env: dict[str, str] | None = None) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        env=env,
    )
    output, _ = await proc.communicate()
    return proc.returncode, output.decode("utf-8", "replace")[-4000:]


async def backup_worker() -> None:
    """Run once at startup, then every 24h. BACKUP_DIR must be a persistent mounted volume."""
    database_url = os.getenv("DATABASE_URL", "").strip()
    backup_dir = os.getenv("BACKUP_DIR", "/app/backups").strip()
    restore_url = os.getenv("BACKUP_RESTORE_TEST_DATABASE_URL", "").strip()
    if not database_url or not backup_dir:
        log.warning("Automated backups disabled: DATABASE_URL is unavailable")
        return
    if not restore_url:
        log.warning("No isolated restore-test database configured; archives will be structurally validated only")
    if restore_url and restore_url == database_url:
        log.error("Restore drill refused: test database URL equals production DATABASE_URL")
        restore_url = ""

    directory = Path(backup_dir)
    directory.mkdir(parents=True, exist_ok=True)
    while True:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive = directory / f"gdewtf-{stamp}.dump"
        env = os.environ.copy()
        env["PGCONNECT_TIMEOUT"] = "10"
        try:
            code, output = await _run(
                "pg_dump", "--format=custom", "--no-owner", "--no-acl",
                "--file", str(archive), database_url, env=env,
            )
            if code != 0:
                raise RuntimeError("pg_dump failed: " + output)
            os.chmod(archive, 0o600)
            code, output = await _run("pg_restore", "--list", str(archive), env=env)
            if code != 0:
                raise RuntimeError("backup archive validation failed: " + output)
            log.info("PostgreSQL backup archive validated: %s (%s bytes)", archive.name, archive.stat().st_size)

            if restore_url:
                code, output = await _run(
                    "pg_restore", "--clean", "--if-exists", "--no-owner", "--no-acl",
                    "--dbname", restore_url, str(archive), env=env,
                )
                if code != 0:
                    raise RuntimeError("isolated restore drill failed: " + output)
                code, output = await _run("psql", restore_url, "-X", "-A", "-t", "-c", "SELECT 1", env=env)
                if code != 0 or output.strip().splitlines()[-1:] != ["1"]:
                    raise RuntimeError("restore drill health query failed: " + output)
                log.info("PostgreSQL restore drill passed against the separate test database")

            # Keep two weeks of daily archives. Never delete the current archive.
            cutoff = datetime.now(timezone.utc).timestamp() - 14 * 86400
            for item in directory.glob("gdewtf-*.dump"):
                try:
                    if item != archive and item.stat().st_mtime < cutoff:
                        item.unlink()
                except OSError:
                    log.exception("Could not prune old backup %s", item)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("PostgreSQL backup/verification failed")
            try:
                archive.unlink(missing_ok=True)
            except OSError:
                pass
        await asyncio.sleep(86400)
