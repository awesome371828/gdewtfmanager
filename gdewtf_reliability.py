"""Durable Telegram update deduplication and GDE.WTF operation journal.

The journal deliberately distinguishes a definite failure from an ambiguous
network outcome. Ambiguous writes are never replayed automatically: the remote
GDE.WTF API does not document an idempotency-key contract.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from aiogram import BaseMiddleware
from aiogram.types import Update

log = logging.getLogger("gdewtf.reliability")


class PersistentUpdateDeduplicationMiddleware(BaseMiddleware):
    """Persist Telegram update IDs before dispatch; acknowledge only completed IDs."""
    def __init__(self, database: Any, *, stale_after_seconds: int = 900) -> None:
        self.database = database
        self.stale_after_seconds = stale_after_seconds

    def _claim(self, update_id: int) -> bool:
        # A completed update or a recently claimed update must never execute twice.
        try:
            self.database.table("bot_telegram_updates").insert({
                "update_id": int(update_id), "status": "processing",
            }).execute()
            return True
        except Exception as exc:
            # A uniqueness conflict means another worker/process already claimed it.
            # Only reclaim a stale processing row; do not replay completed updates.
            name = type(exc).__name__.casefold()
            if "unique" not in name and "duplicate" not in str(exc).casefold():
                raise
        rows = (self.database.table("bot_telegram_updates")
                .select("status,updated_at")
                .eq("update_id", int(update_id)).limit(1).execute().data or [])
        if not rows:
            return False
        row = rows[0]
        if row.get("status") == "done":
            return False
        # An interrupted processing update is not replayed blindly: the handler may
        # have completed an external side effect before the process died.
        return False

    def _finish(self, update_id: int, status: str, error: str | None = None) -> None:
        self.database.table("bot_telegram_updates").update({
            "status": status, "last_error": error,
        }).eq("update_id", int(update_id)).execute()

    async def __call__(self, handler, event, data):
        update_id = getattr(event, "update_id", None)
        if update_id is None:
            return await handler(event, data)
        claimed = await asyncio.to_thread(self._claim, int(update_id))
        if not claimed:
            log.warning("Skipping duplicate or previously interrupted Telegram update_id=%s", update_id)
            return None
        try:
            result = await handler(event, data)
        except BaseException as exc:
            # Keep the row as processing. Re-execution could duplicate a ban/warn.
            await asyncio.to_thread(
                self._finish, int(update_id), "uncertain",
                f"{type(exc).__name__}: {str(exc)[:400]}",
            )
            raise
        await asyncio.to_thread(self._finish, int(update_id), "done", None)
        return result


def enqueue_operation(database: Any, *, event_key: str, chat_id: int,
                      actor_id: int, target_id: int, event_type: str,
                      payload: dict[str, Any]) -> dict[str, Any]:
    """Insert an operation intent once; caller must not replay uncertain writes."""
    row = {
        "event_key": event_key, "chat_id": int(chat_id), "actor_id": int(actor_id),
        "target_id": int(target_id), "event_type": event_type, "payload": payload,
        "status": "pending",
    }
    result = database.table("bot_gdewtf_sync_outbox").upsert(
        row, on_conflict="event_key"
    ).execute()
    return (result.data or [{}])[0]


def mark_operation_uncertain(database: Any, event_key: str, error: str) -> None:
    """Persist an ambiguous outcome for manual/reconciliation handling, not retry."""
    database.table("bot_gdewtf_sync_outbox").update({
        "status": "dead", "last_error": ("UNCERTAIN: " + error)[:1000],
        "updated_at": "now()",
    }).eq("event_key", event_key).execute()


class LatencyProfiler:
    """Low-overhead in-process rolling latency aggregates for command/SQL spans."""
    def __init__(self, *, sample_limit: int = 512) -> None:
        from collections import defaultdict, deque
        self.samples = defaultdict(lambda: deque(maxlen=sample_limit))

    def observe(self, name: str, elapsed_seconds: float) -> None:
        self.samples[name].append(max(0.0, float(elapsed_seconds)))

    def snapshot(self) -> dict[str, dict[str, float | int]]:
        result = {}
        for name, values in self.samples.items():
            ordered = sorted(values)
            if not ordered:
                continue
            result[name] = {
                "count": len(ordered),
                "p50_ms": round(ordered[int((len(ordered) - 1) * .50)] * 1000, 1),
                "p95_ms": round(ordered[int((len(ordered) - 1) * .95)] * 1000, 1),
                "max_ms": round(ordered[-1] * 1000, 1),
            }
        return result
