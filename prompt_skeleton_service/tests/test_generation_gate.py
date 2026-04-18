from __future__ import annotations

from threading import Thread
from time import sleep
from unittest import TestCase

from app.core.exceptions import DomainError
from app.services.generation_gate import GenerationGate


class GenerationGateTest(TestCase):
    def test_waiting_request_acquires_after_release(self) -> None:
        gate = GenerationGate(max_concurrent=1, max_waiting=2, acquire_timeout_seconds=1)
        first = gate.acquire()
        observed: dict[str, float | int] = {}

        def worker() -> None:
            lease = gate.acquire()
            observed["queue_position"] = lease.queue_position
            observed["wait_seconds"] = lease.wait_seconds
            lease.release()

        thread = Thread(target=worker)
        thread.start()
        sleep(0.15)
        first.release()
        thread.join(timeout=2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(observed["queue_position"], 1)
        self.assertGreater(observed["wait_seconds"], 0)

    def test_queue_full_returns_domain_error(self) -> None:
        gate = GenerationGate(max_concurrent=1, max_waiting=1, acquire_timeout_seconds=1)
        first = gate.acquire()
        second_thread_started = []

        def waiting_worker() -> None:
            second_thread_started.append(True)
            lease = gate.acquire()
            sleep(0.05)
            lease.release()

        thread = Thread(target=waiting_worker)
        thread.start()
        sleep(0.15)
        self.assertTrue(second_thread_started)

        with self.assertRaises(DomainError) as ctx:
            gate.acquire()

        self.assertEqual(ctx.exception.details["reason"], "queue_full")
        first.release()
        thread.join(timeout=2)
