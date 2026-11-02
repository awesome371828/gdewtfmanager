"""Short-lived, one-shot confirmation tokens for destructive chat actions."""
from __future__ import annotations
import secrets
import time
from typing import Any

class ConfirmationStore:
    def __init__(self, *, ttl: float = 45.0, capacity: int = 500):
        self.ttl = float(ttl)
        self.capacity = int(capacity)
        self._items: dict[str, dict[str, Any]] = {}

    def create(self, *, user_id: int, chat_id: int, message_id: int, command: str, reply_to_message: Any = None) -> str:
        self.prune()
        if len(self._items) >= self.capacity:
            oldest = min(self._items, key=lambda k: self._items[k]["created"])
            self._items.pop(oldest, None)
        token = secrets.token_urlsafe(8)
        now = time.monotonic()
        self._items[token] = {
            "user_id": int(user_id), "chat_id": int(chat_id),
            "message_id": int(message_id), "command": str(command),
            "created": now, "expires": now + self.ttl, "reply_to_message": reply_to_message,
        }
        return token

    def consume(self, token: str, *, user_id: int, chat_id: int) -> dict[str, Any] | None:
        item = self._items.get(token)
        if item is None:
            return None
        if item["expires"] <= time.monotonic():
            self._items.pop(token, None)
            return None
        if item["user_id"] != int(user_id) or item["chat_id"] != int(chat_id):
            return None
        return self._items.pop(token, None)

    def cancel(self, token: str, *, user_id: int, chat_id: int) -> bool:
        item = self.consume(token, user_id=user_id, chat_id=chat_id)
        return item is not None

    def prune(self) -> None:
        now = time.monotonic()
        for token, item in list(self._items.items()):
            if item["expires"] <= now:
                self._items.pop(token, None)
