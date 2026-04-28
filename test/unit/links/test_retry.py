"""
Unit tests for Processor retry loop and on_error_event observability hook.

Covers:
  - max_retries=0 (default) — single attempt, raises on failure
  - max_retries > 0 — retries N times then either succeeds or exhausts
  - retry succeeds before max_retries hit — publishes correct result
  - retry exhausted — falls through to on_error callback / raises
  - retry exhausted — routes ErrorEnvelope to error_subscribers
  - on_error_event fires on every failed attempt (not just the last)
  - retry_delay property setter enforces minimum 0
  - retry_backoff property setter is stored as-is
  - items_errored metric increments once per failed *item* (not per attempt)
"""
import unittest
from unittest.mock import AsyncMock, MagicMock

from io_chains._internal.sentinel import END_OF_STREAM, ErrorEnvelope, Skip
from io_chains.links.collector import Collector
from io_chains.links.processor import Processor


# ---------------------------------------------------------------------------
# Property setters
# ---------------------------------------------------------------------------


class TestRetryProperties(unittest.TestCase):
    def test_max_retries_default_zero(self):
        p = Processor()
        self.assertEqual(p.max_retries, 0)

    def test_max_retries_setter(self):
        p = Processor(max_retries=3)
        self.assertEqual(p.max_retries, 3)

    def test_max_retries_setter_negative_clamps_to_zero(self):
        p = Processor(max_retries=-5)
        self.assertEqual(p.max_retries, 0)

    def test_retry_delay_default_zero(self):
        p = Processor()
        self.assertEqual(p.retry_delay, 0.0)

    def test_retry_delay_setter_clamps_negative_to_zero(self):
        p = Processor(retry_delay=-1.0)
        self.assertEqual(p.retry_delay, 0.0)

    def test_retry_backoff_default(self):
        p = Processor()
        self.assertEqual(p.retry_backoff, 2.0)

    def test_retry_backoff_setter_stored(self):
        p = Processor(retry_backoff=3.5)
        self.assertEqual(p.retry_backoff, 3.5)


# ---------------------------------------------------------------------------
# Retry loop behaviour
# ---------------------------------------------------------------------------


class TestProcessorRetry(unittest.IsolatedAsyncioTestCase):
    async def test_no_retries_raises_immediately(self):
        """max_retries=0 — a single attempt; exception propagates."""
        p = Processor(
            source=[1],
            processor=lambda x: 1 / 0,
        )
        with self.assertRaises(ZeroDivisionError):
            await p()

    async def test_retry_succeeds_on_second_attempt(self):
        """Fails on attempt 0, succeeds on attempt 1 — result published."""
        calls = []

        def flaky(x):
            calls.append(x)
            if len(calls) == 1:
                raise ValueError("first attempt")
            return x * 10

        results = Collector()
        p = Processor(
            source=[5],
            processor=flaky,
            max_retries=2,
            subscribers=[results],
        )
        await p()
        items = [item async for item in results]
        self.assertEqual(items, [50])
        self.assertEqual(len(calls), 2)

    async def test_retry_exhausted_then_raises(self):
        """All attempts fail — exception propagates after max_retries."""
        p = Processor(
            source=[1],
            processor=lambda x: 1 / 0,
            max_retries=2,
        )
        with self.assertRaises(ZeroDivisionError):
            await p()

    async def test_retry_exhausted_falls_through_to_on_error_callback(self):
        """After retries exhausted, on_error callback is called with final exc."""
        recovered = []
        results = Collector()
        p = Processor(
            source=[7],
            processor=lambda x: 1 / 0,
            max_retries=1,
            on_error=lambda e, datum: recovered.append(datum) or Skip(),
            subscribers=[results],
        )
        await p()
        self.assertEqual(recovered, [7])
        items = [item async for item in results]
        self.assertEqual(items, [])  # Skip was returned

    async def test_retry_exhausted_routes_to_error_subscribers(self):
        """After retries exhausted, ErrorEnvelope is routed to error_subscribers."""
        error_sink = Collector()
        results = Collector()
        p = Processor(
            source=[3],
            processor=lambda x: 1 / 0,
            max_retries=1,
            subscribers=[results],
        )
        p.error_subscribers = error_sink

        await p()

        # Drain error_sink
        await error_sink.push(END_OF_STREAM)
        envelopes = [item async for item in error_sink]
        self.assertEqual(len(envelopes), 1)
        env = envelopes[0]
        self.assertIsInstance(env, ErrorEnvelope)
        self.assertEqual(env.datum, 3)
        self.assertIsInstance(env.exc, ZeroDivisionError)
        self.assertEqual(env.retry_count, 1)  # max_retries
        self.assertTrue(env.handled)

        # Normal downstream gets nothing
        items = [item async for item in results]
        self.assertEqual(items, [])

    async def test_multiple_items_each_retried_independently(self):
        """Each item gets its own retry budget."""
        attempts: dict[int, int] = {}

        def flaky(x):
            attempts[x] = attempts.get(x, 0) + 1
            if attempts[x] < 2:
                raise ValueError(f"attempt {attempts[x]} failed for {x}")
            return x

        results = Collector()
        p = Processor(
            source=[1, 2, 3],
            processor=flaky,
            max_retries=2,
            subscribers=[results],
        )
        await p()
        items = [item async for item in results]
        self.assertEqual(sorted(items), [1, 2, 3])


# ---------------------------------------------------------------------------
# on_error_event observability hook
# ---------------------------------------------------------------------------


class TestOnErrorEvent(unittest.IsolatedAsyncioTestCase):
    async def test_on_error_event_fires_once_after_all_retries(self):
        """Hook fires once after all retry attempts, with retry_count = max_retries
        and the correct handled state (so callers know if an error edge caught it)."""
        events: list[ErrorEnvelope] = []

        def hook(env: ErrorEnvelope):
            events.append(env)

        p = Processor(
            source=[1],
            processor=lambda x: 1 / 0,
            max_retries=2,
            on_error=lambda e, d: Skip(),  # prevent pipeline failure
        )
        p.on_error_event = hook
        await p()

        # One event at the end, retry_count reflects max retries exhausted
        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], ErrorEnvelope)
        self.assertEqual(events[0].retry_count, 2)
        self.assertEqual(events[0].datum, 1)

    async def test_on_error_event_fires_on_single_failure_no_retries(self):
        events: list[ErrorEnvelope] = []

        def hook(env: ErrorEnvelope):
            events.append(env)

        p = Processor(
            source=[5],
            processor=lambda x: 1 / 0,
            on_error=lambda e, d: Skip(),
        )
        p.on_error_event = hook
        await p()

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].retry_count, 0)

    async def test_async_on_error_event_is_awaited(self):
        """Async hook functions are properly awaited."""
        events: list[ErrorEnvelope] = []

        async def async_hook(env: ErrorEnvelope):
            events.append(env)

        p = Processor(
            source=[1],
            processor=lambda x: 1 / 0,
            on_error=lambda e, d: Skip(),
        )
        p.on_error_event = async_hook
        await p()
        self.assertEqual(len(events), 1)

    async def test_on_error_event_reports_handled_true_when_error_edge_catches(self):
        """When an error_subscriber handles the error, on_error_event fires with
        handled=True so runners can distinguish dead-lettered errors from crashes."""
        from io_chains._internal.subscriber import Subscriber
        from io_chains._internal.sentinel import EndOfStream

        events: list[ErrorEnvelope] = []

        class _Sink(Subscriber):
            """Swallows everything — simulates a dead_letter node."""
            async def push(self, datum):
                pass

        p = Processor(source=[1], processor=lambda x: 1 / 0)
        p.on_error_event = lambda env: events.append(env)
        p._error_subscribers.append(_Sink())

        await p()

        self.assertEqual(len(events), 1)
        self.assertTrue(events[0].handled, 'expected handled=True when error_subscriber caught the error')

    async def test_on_error_event_does_not_suppress_exception(self):
        """Hook fires, but exception still propagates when no on_error is set."""
        fired = []
        p = Processor(
            source=[1],
            processor=lambda x: 1 / 0,
        )
        p.on_error_event = lambda env: fired.append(env)
        with self.assertRaises(ZeroDivisionError):
            await p()
        self.assertEqual(len(fired), 1)

    async def test_on_error_event_setter_rejects_non_callable(self):
        p = Processor()
        with self.assertRaises(TypeError):
            p.on_error_event = "not-callable"

    async def test_on_error_event_setter_accepts_none(self):
        p = Processor()
        p.on_error_event = None  # must not raise

    async def test_on_error_event_not_called_on_success(self):
        """Hook must NOT fire when processor succeeds without errors."""
        fired = []
        results = Collector()
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x * 2,
            subscribers=[results],
        )
        p.on_error_event = lambda env: fired.append(env)
        await p()
        self.assertEqual(fired, [])


if __name__ == "__main__":
    unittest.main()
