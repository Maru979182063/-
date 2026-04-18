from __future__ import annotations

import time
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Condition

from app.core.exceptions import DomainError
from app.core.settings import get_settings


@dataclass(slots=True)
class GateLease:
    gate: "GenerationGate"
    queue_position: int
    wait_seconds: float

    def release(self) -> None:
        self.gate.release()


class GenerationGate:
    def __init__(self, *, max_concurrent: int, max_waiting: int, acquire_timeout_seconds: int) -> None:
        self.max_concurrent = max(1, max_concurrent)
        self.max_waiting = max(0, max_waiting)
        self.acquire_timeout_seconds = max(1, acquire_timeout_seconds)
        self._condition = Condition()
        self._active = 0
        self._waiting = 0

    def snapshot(self) -> dict[str, int]:
        with self._condition:
            return self._snapshot_unlocked()

    def acquire(self) -> GateLease:
        started = time.monotonic()
        with self._condition:
            if self._active < self.max_concurrent:
                self._active += 1
                return GateLease(self, queue_position=0, wait_seconds=0.0)

            if self._waiting >= self.max_waiting:
                raise DomainError(
                    "Generation queue is full.",
                    status_code=503,
                    details={**self._snapshot_unlocked(), "reason": "queue_full"},
                )

            self._waiting += 1
            queue_position = self._waiting
            deadline = started + self.acquire_timeout_seconds
            try:
                while self._active >= self.max_concurrent:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise DomainError(
                            "Generation queue wait timed out.",
                            status_code=503,
                            details={
                                **self._snapshot_unlocked(),
                                "reason": "queue_timeout",
                                "queue_position": queue_position,
                            },
                        )
                    self._condition.wait(timeout=remaining)
                self._active += 1
                return GateLease(
                    self,
                    queue_position=queue_position,
                    wait_seconds=max(0.0, time.monotonic() - started),
                )
            finally:
                self._waiting -= 1

    def release(self) -> None:
        with self._condition:
            if self._active > 0:
                self._active -= 1
            self._condition.notify_all()

    def _snapshot_unlocked(self) -> dict[str, int]:
        return {
            "active_requests": self._active,
            "waiting_requests": self._waiting,
            "max_concurrent": self.max_concurrent,
            "max_waiting": self.max_waiting,
        }


_GENERATION_GATE: GenerationGate | None = None


def get_generation_gate() -> GenerationGate:
    global _GENERATION_GATE
    if _GENERATION_GATE is None:
        queue = get_settings().generation_queue
        _GENERATION_GATE = GenerationGate(
            max_concurrent=queue.max_concurrent,
            max_waiting=queue.max_waiting,
            acquire_timeout_seconds=queue.acquire_timeout_seconds,
        )
    return _GENERATION_GATE


@contextmanager
def acquire_generation_slot():
    lease = get_generation_gate().acquire()
    try:
        yield {
            "queue_position": lease.queue_position,
            "wait_seconds": round(lease.wait_seconds, 4),
            **get_generation_gate().snapshot(),
        }
    finally:
        lease.release()
