"""
Unit tests for Link observability: name, metrics tracking, callbacks, and properties.

Covers:
  - name param is stored and accessible; default is empty string
  - items_in, items_out, items_skipped, items_errored counted correctly
  - elapsed_seconds is positive after a run
  - on_metrics callback called once at EOS with a LinkMetrics instance
  - on_error property validates callable-or-None
  - Read-only properties: items_in, items_skipped, items_errored, upstream_count, queue_depth_max
  - Extended LinkMetrics fields: memory_peak_bytes, time_per_item_seconds,
    subscribed_count, subscriber_count, queue_depth_max, throughput_items_per_sec
  - Structured log emitted at EOS with all metric fields in extra
  - Enricher participates in metrics
"""

import logging
import unittest

from io_chains.links.enricher import Enricher
from io_chains.links.processor import Processor
from io_chains.links.collector import Collector
from io_chains._internal.sentinel import Skip


class TestLinkName(unittest.TestCase):
    def test_processor_stores_name(self):
        p = Processor(name="my-processor")
        self.assertEqual(p.name, "my-processor")

    def test_default_name_is_empty_string(self):
        p = Processor()
        self.assertEqual(p.name, "")

    def test_enricher_stores_name(self):
        e = Enricher(
            name="my-enricher",
            relations=[],
            primary_channel="primary",
        )
        self.assertEqual(e.name, "my-enricher")


class TestMetricsTracking(unittest.IsolatedAsyncioTestCase):
    async def test_items_in_counts_received_items(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_in, 3)

    async def test_items_out_counts_published_items(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_out, 3)

    async def test_items_skipped_counts_skip_results(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: Skip() if x == 2 else x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_skipped, 1)
        self.assertEqual(captured[0].items_out, 2)

    async def test_items_errored_counts_handled_errors(self):
        captured = []

        def boom(x):
            if x == 2:
                raise ValueError("bad item")
            return x

        p = Processor(
            source=[1, 2, 3],
            processor=boom,
            on_error=lambda e, datum: datum,  # recover: pass item through
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_errored, 1)
        self.assertEqual(captured[0].items_out, 3)  # all 3 still published

    async def test_elapsed_seconds_is_positive(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertGreater(captured[0].elapsed_seconds, 0)

    async def test_metrics_has_correct_name(self):
        captured = []
        p = Processor(
            source=[1],
            processor=lambda x: x,
            name="named-link",
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].name, "named-link")

    async def test_on_metrics_called_exactly_once(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            processor=lambda x: x,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(len(captured), 1)

    async def test_generator_processor_counts_multiple_outputs(self):
        """A processor that yields multiple items: items_out > items_in."""
        captured = []

        def expand(x):
            yield x
            yield x * 10

        p = Processor(
            source=[1, 2],
            processor=expand,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_in, 2)
        self.assertEqual(captured[0].items_out, 4)

    async def test_no_processor_passthrough_still_tracked(self):
        captured = []
        p = Processor(
            source=[1, 2, 3],
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_in, 3)
        self.assertEqual(captured[0].items_out, 3)

    async def test_enricher_metrics_tracked(self):
        captured = []
        results = Collector()

        enricher = Enricher(
            relations=[],
            primary_channel="primary",
            subscribers=[results],
            on_metrics=lambda m: captured.append(m),
        )

        feeder = Processor(source=[{"id": 1}, {"id": 2}])
        feeder.subscribe(enricher, channel="primary")

        from asyncio import create_task, gather

        await gather(create_task(feeder()), create_task(enricher()))

        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].items_in, 2)
        self.assertEqual(captured[0].items_out, 2)

    async def test_multiple_workers_aggregate_metrics(self):
        """items_in and items_out are correct totals across all workers."""
        captured = []
        p = Processor(
            source=list(range(10)),
            processor=lambda x: x,
            workers=4,
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].items_in, 10)
        self.assertEqual(captured[0].items_out, 10)


class TestStructuredLogging(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._links_logger = logging.getLogger("io_chains")
        self._original_level = self._links_logger.level
        self._links_logger.setLevel(logging.DEBUG)

    def tearDown(self):
        self._links_logger.setLevel(self._original_level)

    async def test_log_emitted_at_eos(self):
        handler = _CapturingHandler()
        self._links_logger.addHandler(handler)
        try:
            p = Processor(source=[1, 2, 3], processor=lambda x: x, name="log-test")
            await p()
        finally:
            self._links_logger.removeHandler(handler)

        self.assertTrue(len(handler.records) > 0)
        names = [r.getMessage() for r in handler.records]
        self.assertTrue(any("log-test" in msg for msg in names))

    async def test_log_record_contains_metrics_extra(self):
        handler = _CapturingHandler()
        self._links_logger.addHandler(handler)
        try:
            p = Processor(source=[1, 2], processor=lambda x: x, name="metrics-log")
            await p()
        finally:
            self._links_logger.removeHandler(handler)

        eos_records = [r for r in handler.records if hasattr(r, "items_in")]
        self.assertTrue(len(eos_records) > 0)
        r = eos_records[0]
        self.assertEqual(r.items_in, 2)
        self.assertEqual(r.items_out, 2)


class TestLinkReadOnlyProperties(unittest.IsolatedAsyncioTestCase):
    async def _run_processor(self, items, processor=None, **kwargs):
        p = Processor(source=items, processor=processor or (lambda x: x), **kwargs)
        await p()
        return p

    async def test_items_in_property(self):
        p = await self._run_processor([1, 2, 3])
        self.assertEqual(p.items_in, 3)

    async def test_items_skipped_property(self):
        from io_chains._internal.sentinel import Skip
        p = await self._run_processor([1, 2, 3], processor=lambda x: Skip() if x == 2 else x)
        self.assertEqual(p.items_skipped, 1)

    async def test_items_errored_property(self):
        def boom(x):
            if x == 2:
                raise ValueError
            return x
        p = await self._run_processor(
            [1, 2, 3],
            processor=boom,
            on_error=lambda e, d: d,
        )
        self.assertEqual(p.items_errored, 1)

    async def test_upstream_count_property(self):
        p1 = Processor(source=[1], processor=lambda x: x)
        p2 = Processor(processor=lambda x: x)
        p1.subscribers = [p2]
        import asyncio
        await asyncio.gather(asyncio.create_task(p1()), asyncio.create_task(p2()))
        self.assertEqual(p2.upstream_count, 1)

    async def test_queue_depth_max_property(self):
        p = await self._run_processor([1, 2, 3])
        self.assertGreaterEqual(p.queue_depth_max, 1)

    def test_on_error_accepts_callable(self):
        cb = lambda e, d: d
        p = Processor(on_error=cb)
        self.assertIs(p.on_error, cb)

    def test_on_error_accepts_none(self):
        p = Processor()
        p.on_error = None
        self.assertIsNone(p.on_error)

    def test_on_error_rejects_non_callable(self):
        p = Processor()
        with self.assertRaises(TypeError):
            p.on_error = 42

    def test_on_error_set_via_constructor(self):
        cb = lambda e, d: d
        p = Processor(on_error=cb)
        self.assertIs(p.on_error, cb)


class TestExtendedMetrics(unittest.IsolatedAsyncioTestCase):
    async def test_subscribed_count_reflects_upstream_count(self):
        captured = []
        p1 = Processor(source=[1, 2], processor=lambda x: x)
        p2 = Processor(processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        p1.subscribers = [p2]

        import asyncio
        await asyncio.gather(asyncio.create_task(p1()), asyncio.create_task(p2()))
        self.assertEqual(captured[0].subscribed_count, 1)

    async def test_subscribed_count_zero_for_source_link(self):
        captured = []
        p = Processor(source=[1, 2], processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        await p()
        self.assertEqual(captured[0].subscribed_count, 0)

    async def test_subscriber_count_reflects_downstream_count(self):
        captured = []
        sink1 = Collector()
        sink2 = Collector()
        p = Processor(
            source=[1, 2],
            processor=lambda x: x,
            subscribers=[sink1, sink2],
            on_metrics=lambda m: captured.append(m),
        )
        await p()
        self.assertEqual(captured[0].subscriber_count, 2)

    async def test_subscriber_count_zero_when_no_subscribers(self):
        captured = []
        p = Processor(source=[1, 2], processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        await p()
        self.assertEqual(captured[0].subscriber_count, 0)

    async def test_time_per_item_seconds_positive(self):
        captured = []
        p = Processor(source=[1, 2, 3], processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        await p()
        self.assertGreater(captured[0].time_per_item_seconds, 0)

    async def test_time_per_item_zero_when_no_items(self):
        captured = []
        p = Processor(source=[], processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        await p()
        self.assertEqual(captured[0].time_per_item_seconds, 0.0)

    async def test_throughput_items_per_sec_positive(self):
        captured = []
        p = Processor(source=[1, 2, 3], processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        await p()
        self.assertGreater(captured[0].throughput_items_per_sec, 0)

    async def test_queue_depth_max_at_least_one_when_items_processed(self):
        captured = []
        p = Processor(source=[1, 2, 3], processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        await p()
        self.assertGreaterEqual(captured[0].queue_depth_max, 1)

    async def test_queue_depth_max_zero_when_no_items(self):
        captured = []
        p = Processor(source=[], processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        await p()
        self.assertEqual(captured[0].queue_depth_max, 0)

    async def test_memory_peak_bytes_zero_when_not_tracing(self):
        import tracemalloc
        if tracemalloc.is_tracing():
            self.skipTest("tracemalloc already active")
        captured = []
        p = Processor(source=[1, 2, 3], processor=lambda x: x, on_metrics=lambda m: captured.append(m))
        await p()
        self.assertEqual(captured[0].memory_peak_bytes, 0)

    async def test_memory_peak_bytes_nonnegative_when_tracing(self):
        import tracemalloc
        tracemalloc.start()
        try:
            captured = []
            p = Processor(source=list(range(100)), processor=lambda x: x, on_metrics=lambda m: captured.append(m))
            await p()
        finally:
            tracemalloc.stop()
        self.assertGreaterEqual(captured[0].memory_peak_bytes, 0)


class _CapturingHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


if __name__ == "__main__":
    unittest.main()
