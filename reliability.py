"""Runtime resilience primitives for the GDE.WTF bot.

No network or database dependencies: the primitives are deterministic and easy
to test. Mutating API calls are never retried automatically by this module.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque


class CircuitOpenError(RuntimeError):
    """Raised when an upstream is temporarily protected by the circuit breaker."""

    def __init__(self, retry_after: float):
        self.retry_after = max(0.0, float(retry_after))
        super().__init__(f"circuit open; retry after {self.retry_after:.2f}s")


@dataclass(frozen=True)
class CircuitSnapshot:
    state: str
    failures: int
    opened_for: float
    probe_in_flight: bool


class AsyncCircuitBreaker:
    """Small async circuit breaker with a single half-open probe.

    Call before an upstream request, then report exactly one success/failure.
    Caller cancellation must not be counted as an upstream failure.
    """

    def __init__(self, *, failure_threshold: int = 5, recovery_timeout: float = 12.0):
        if failure_threshold < 1 or recovery_timeout <= 0:
            raise ValueError("invalid circuit breaker configuration")
        self.failure_threshold = int(failure_threshold)
        self.recovery_timeout = float(recovery_timeout)
        self._failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False
        self._lock = asyncio.Lock()

    async def before_call(self) -> None:
        async with self._lock:
            if not self._opened_at:
                return
            elapsed = time.monotonic() - self._opened_at
            if elapsed < self.recovery_timeout:
                raise CircuitOpenError(self.recovery_timeout - elapsed)
            if self._probe_in_flight:
                raise CircuitOpenError(0.5)
            self._probe_in_flight = True

    async def record_success(self) -> None:
        async with self._lock:
            self._failures = 0
            self._opened_at = 0.0
            self._probe_in_flight = False

    async def record_failure(self) -> None:
        async with self._lock:
            self._probe_in_flight = False
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._opened_at = time.monotonic()

    async def snapshot(self) -> CircuitSnapshot:
        async with self._lock:
            if not self._opened_at:
                state = "closed"
                opened_for = 0.0
            else:
                opened_for = max(0.0, time.monotonic() - self._opened_at)
                state = "half_open" if opened_for >= self.recovery_timeout else "open"
            return CircuitSnapshot(state, self._failures, opened_for, self._probe_in_flight)


class RollingLatency:
    """Bounded latency/error sample window for lightweight diagnostics."""

    def __init__(self, capacity: int = 256):
        self._samples: Deque[tuple[float, bool]] = deque(maxlen=max(8, int(capacity)))
        self._lock = asyncio.Lock()

    async def add(self, duration: float, ok: bool) -> None:
        async with self._lock:
            self._samples.append((max(0.0, float(duration)), bool(ok)))

    async def snapshot(self) -> dict[str, float | int]:
        async with self._lock:
            values = sorted(sample[0] for sample in self._samples)
            count = len(values)
            if not count:
                return {"count": 0, "errors": 0, "p50_ms": 0, "p95_ms": 0, "max_ms": 0}
            def percentile(p: float) -> float:
                index = min(count - 1, max(0, int((count - 1) * p + 0.999999)))
                return round(values[index] * 1000, 1)
            errors = sum(1 for _, ok in self._samples if not ok)
            return {
                "count": count,
                "errors": errors,
                "p50_ms": percentile(0.50),
                "p95_ms": percentile(0.95),
                "max_ms": round(values[-1] * 1000, 1),
            }
