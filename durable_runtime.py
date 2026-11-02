"""Durable PostgreSQL runtime guards for Telegram updates and upstream mutations.

The module deliberately never retries an uncertain mutation. A timeout is recorded
as unknown and must be reconciled against the upstream before an operator retries it.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from typing import Any


class DurableRuntime:
    def __init__(self, db: Any, *, lease_seconds: int = 90):
        self.db = db
        self.lease_seconds = int(lease_seconds)

    def _run(self, sql: str, params: tuple = (), *, fetch: bool = False):
        with self.db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                result = cur.fetchall() if fetch == "all" else (cur.fetchone() if fetch else None)
            conn.commit()
            return result

    async def claim_update(self, update_id: int) -> bool:
        def claim():
            row = self._run(
                """INSERT INTO public.bot_telegram_updates
                       (update_id, status, received_at, lease_until)
                   VALUES (%s, 'processing', now(), now() + (%s * interval '1 second'))
                   ON CONFLICT (update_id) DO UPDATE
                     SET status='processing', received_at=now(),
                         lease_until=now() + (%s * interval '1 second'),
                         last_error=NULL
                   WHERE public.bot_telegram_updates.status='failed'
                      OR (public.bot_telegram_updates.status='processing'
                          AND public.bot_telegram_updates.lease_until < now())
                   RETURNING update_id""",
                (int(update_id), self.lease_seconds, self.lease_seconds), fetch=True,
            )
            return row is not None
        return await asyncio.to_thread(claim)

    async def finish_update(self, update_id: int, error: str | None = None) -> None:
        status = "failed" if error else "processed"
        await asyncio.to_thread(
            self._run,
            """UPDATE public.bot_telegram_updates
               SET status=%s, processed_at=CASE WHEN %s='processed' THEN now() ELSE NULL END,
                   lease_until=NULL, last_error=%s
               WHERE update_id=%s AND status='processing'""",
            (status, status, str(error)[:1000] if error else None, int(update_id)),
        )

    async def begin_operation(self, method: str, path: str, payload: dict[str, Any] | None) -> str:
        operation_id = str(uuid.uuid4())
        digest = hashlib.sha256(
            (method.upper() + "\\n" + path + "\\n" +
             json.dumps(payload or {}, sort_keys=True, ensure_ascii=False, default=str)).encode()
        ).hexdigest()
        await asyncio.to_thread(
            self._run,
            """INSERT INTO public.bot_gdewtf_operations
               (operation_id, idempotency_key, action, payload, status, attempts)
               VALUES (%s, %s, %s, %s::jsonb, 'processing', 1)""",
            (operation_id, digest + ":" + operation_id, method.upper() + " " + path,
             json.dumps(payload or {}, ensure_ascii=False, default=str)),
        )
        return operation_id

    async def list_unknown_operations(self, limit: int = 20):
        return await asyncio.to_thread(
            lambda: self._run(
                """SELECT operation_id::text, action, created_at, last_error
                   FROM public.bot_gdewtf_operations
                   WHERE status='unknown'
                   ORDER BY updated_at ASC LIMIT %s""",
                (max(1, min(100, int(limit))),), fetch="all",
            )
        )

    async def unknown_operation_count(self) -> int:
        def count():
            with self.db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) AS n FROM public.bot_gdewtf_operations WHERE status='unknown'")
                    return int(cur.fetchone()["n"])
        return await asyncio.to_thread(count)

    async def finish_operation(self, operation_id: str, status: str, error: str | None = None) -> None:
        if status not in {"success", "failed", "unknown"}:
            raise ValueError("invalid operation terminal status")
        await asyncio.to_thread(
            self._run,
            """UPDATE public.bot_gdewtf_operations
               SET status=%s, last_error=%s, updated_at=now(),
                   completed_at=CASE WHEN %s IN ('success','failed') THEN now() ELSE NULL END,
                   lease_until=NULL
               WHERE operation_id=%s""",
            (status, str(error)[:1500] if error else None, status, operation_id),
        )


class TelegramUpdateDedupMiddleware:
    def __init__(self, runtime: DurableRuntime, logger: Any):
        self.runtime, self.logger = runtime, logger

    async def __call__(self, handler, event, data):
        update_id = getattr(event, "update_id", None)
        if update_id is None:
            return await handler(event, data)
        try:
            claimed = await self.runtime.claim_update(int(update_id))
        except Exception:
            # Database outages must not silently discard all Telegram traffic.
            self.logger.exception("Telegram update dedup unavailable; processing at-least-once")
            return await handler(event, data)
        if not claimed:
            self.logger.info("Duplicate Telegram update ignored: %s", update_id)
            return None
        try:
            result = await handler(event, data)
        except BaseException as exc:
            try:
                await self.runtime.finish_update(update_id, f"{type(exc).__name__}: {exc}")
            except Exception:
                self.logger.exception("Could not persist failed Telegram update %s", update_id)
            raise
        try:
            await self.runtime.finish_update(update_id)
        except Exception:
            # The handler already completed; do not turn a bookkeeping outage into
            # a Telegram retry that could repeat a completed side effect.
            self.logger.exception("Could not persist processed Telegram update %s", update_id)
        return result
