import unittest
from asyncio import create_task, gather
from collections.abc import AsyncGenerator
from logging import getLogger

from io_chains.links.processor import Processor
from io_chains.links.collector import Collector
from io_chains._internal.sentinel import END_OF_STREAM, SKIP

logger = getLogger()


async def gen_func() -> AsyncGenerator[str, None]:
    yield "something"


class TestProcessor(unittest.IsolatedAsyncioTestCase):
    def test_processor_should_instantiate(self):
        actual = Processor()
        self.assertIsInstance(actual, Processor)

    async def test_processor_should_handle_list_source(self):
        results = Collector()
        link = Processor(source=[1, 2, 3], subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([1, 2, 3], actual)

    async def test_processor_should_handle_subscriber(self):
        expected = "something"
        collector = Collector()
        link = Processor(source=[expected], subscribers=[collector])
        await link()
        actual = None
        async for each in collector:
            actual = each
        self.assertEqual(expected, actual)

    async def test_processor_input_should_generate_from_list(self):
        expected = "something"
        link = Processor(source=[expected])
        actual = None
        async for each in link.input:
            actual = each
        self.assertEqual(expected, actual)

    async def test_processor_input_should_generate_from_generator(self):
        expected = "something"
        link = Processor(source=(x for x in [expected]))
        actual = None
        async for each in link.input:
            actual = each
        self.assertEqual(expected, actual)

    async def test_processor_input_should_generate_from_async_gen_func(self):
        expected = "something"
        link = Processor(source=gen_func)
        actual = None
        async for each in link.input:
            actual = each
        self.assertEqual(expected, actual)

    async def test_processor_input_should_generate_from_sync_callable(self):
        # A callable that returns a plain iterable (not an async generator)
        link = Processor(source=lambda: [10, 20, 30])
        actual = [item async for item in link.input]
        self.assertEqual([10, 20, 30], actual)

    async def test_processor_should_publish_to_all_subscribers(self):
        expected = "something"
        sub1 = Collector()
        sub2 = Collector()
        link = Processor(source=[expected], subscribers=[sub1, sub2])
        await link()
        actual1 = None
        async for each in sub1:
            actual1 = each
        actual2 = None
        async for each in sub2:
            actual2 = each
        self.assertEqual(expected, actual1)
        self.assertEqual(expected, actual2)

    async def test_processor_should_support_async_processor(self):
        async def double(x):
            return x * 2

        results = Collector()
        link = Processor(source=[1, 2, 3], processor=double, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([2, 4, 6], actual)

    async def test_processor_subscriber_mode_waits_for_upstream_eos(self):
        # A sourceless processor must NOT emit END_OF_STREAM on its own —
        # it must wait for EOS to arrive via push() from upstream.
        results = Collector()
        link = Processor(processor=lambda x: x * 2, subscribers=[results])
        await link.push(5)
        await link.push(10)
        await link.push(END_OF_STREAM)
        await link()
        actual = [item async for item in results]
        self.assertEqual([10, 20], actual)

    async def test_processor_should_handle_subscriber_link(self):
        expected = "something"
        collector = Collector()
        loader = Processor(processor=lambda x: x, subscribers=[collector])
        extractor = Processor(source=gen_func, subscribers=[loader])
        await gather(
            create_task(extractor()),
            create_task(loader()),
        )
        actual = None
        async for each in collector:
            actual = each
        self.assertEqual(expected, actual)

    async def test_processor_queue_size_applies_backpressure(self):
        # A bounded queue (queue_size=1) should still produce correct results —
        # the producer blocks until the consumer drains each slot.
        results = Collector()
        link = Processor(source=list(range(5)), processor=lambda x: x * 2, subscribers=[results], queue_size=1)
        await link()
        actual = [item async for item in results]
        self.assertEqual([0, 2, 4, 6, 8], actual)

    # --- Filter / drop ---

    async def test_processor_returning_skip_drops_item(self):
        results = Collector()
        link = Processor(
            source=[1, 2, 3, 4, 5],
            processor=lambda x: SKIP if x % 2 == 0 else x,
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([1, 3, 5], actual)

    async def test_processor_skip_all_produces_empty_stream(self):
        results = Collector()
        link = Processor(source=[1, 2, 3], processor=lambda x: SKIP, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([], actual)

    async def test_processor_async_can_return_skip(self):
        results = Collector()

        async def keep_evens(x):
            return x if x % 2 == 0 else SKIP

        link = Processor(source=[1, 2, 3, 4], processor=keep_evens, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([2, 4], actual)

    # --- Flat-map / expand ---

    async def test_processor_sync_generator_expands_items(self):
        results = Collector()
        link = Processor(
            source=["hello world", "foo bar baz"],
            processor=lambda s: (word for word in s.split()),
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual(["hello", "world", "foo", "bar", "baz"], actual)

    async def test_processor_async_generator_expands_items(self):
        results = Collector()

        async def expand(n):
            for i in range(n):
                yield i

        link = Processor(source=[3, 2], processor=expand, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([0, 1, 2, 0, 1], actual)

    async def test_processor_generator_returning_zero_items_filters(self):
        # A generator that yields nothing is equivalent to SKIP
        results = Collector()
        link = Processor(
            source=[1, 2, 3],
            processor=lambda x: (i for i in [] if False),  # always empty
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([], actual)

    # --- Batch processing ---

    async def test_processor_batch_size_groups_items(self):
        results = Collector()
        link = Processor(
            source=list(range(6)),
            processor=lambda batch: sum(batch),
            batch_size=3,
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([3, 12], actual)  # [0+1+2, 3+4+5]

    async def test_processor_batch_partial_batch_flushed_at_eos(self):
        results = Collector()
        link = Processor(
            source=list(range(5)),
            processor=lambda batch: sum(batch),
            batch_size=3,
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([3, 7], actual)  # [0+1+2, 3+4]

    async def test_processor_batch_no_processor_publishes_each_item(self):
        results = Collector()
        link = Processor(source=list(range(4)), batch_size=2, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([0, 1, 2, 3], actual)

    async def test_processor_batch_can_return_skip(self):
        results = Collector()
        # Drop every second batch
        batches_seen = []

        def proc(batch):
            batches_seen.append(batch)
            return SKIP if len(batches_seen) % 2 == 0 else sum(batch)

        link = Processor(source=list(range(6)), processor=proc, batch_size=2, subscribers=[results])
        await link()
        actual = [item async for item in results]
        # batch[0,1]=1 kept, batch[2,3]=5 dropped, batch[4,5]=9 kept
        self.assertEqual([1, 9], actual)

    async def test_processor_batch_can_flat_map(self):
        results = Collector()
        link = Processor(
            source=list(range(4)),
            processor=lambda batch: (x * 2 for x in batch),
            batch_size=2,
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([0, 2, 4, 6], actual)

    # --- Error handling ---

    async def test_processor_on_error_skip_continues_stream(self):
        results = Collector()

        def bad_proc(x):
            if x == 2:
                raise ValueError("bad item")
            return x * 10

        link = Processor(
            source=[1, 2, 3],
            processor=bad_proc,
            on_error=lambda e, item: SKIP,
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([10, 30], actual)

    async def test_processor_on_error_fallback_value(self):
        results = Collector()

        def bad_proc(x):
            if x == 2:
                raise ValueError("bad item")
            return x * 10

        link = Processor(
            source=[1, 2, 3],
            processor=bad_proc,
            on_error=lambda e, item: -1,
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([10, -1, 30], actual)

    async def test_processor_on_error_not_set_propagates_exception(self):
        link = Processor(source=[1], processor=lambda x: 1 / 0)
        with self.assertRaises(ZeroDivisionError):
            await link()

    async def test_processor_on_error_async_handler(self):
        results = Collector()

        async def async_handler(e, item):
            return item  # passthrough on error

        link = Processor(
            source=[1, 2, 3],
            processor=lambda x: 1 / 0,
            on_error=async_handler,
            subscribers=[results],
        )
        await link()
        actual = [item async for item in results]
        self.assertEqual([1, 2, 3], actual)

    # --- Fan-in (multiple upstreams) ---

    async def test_processor_fan_in_waits_for_all_upstream_eos(self):
        # Two upstreams feed one downstream. upstream_count is auto-registered
        # when each upstream wires itself via subscribers=[downstream].
        results = Collector()
        downstream = Processor(processor=lambda x: x, subscribers=[results])
        upstream_a = Processor(source=[1, 2, 3], subscribers=[downstream])
        upstream_b = Processor(source=[4, 5, 6], subscribers=[downstream])
        await gather(
            create_task(upstream_a()),
            create_task(upstream_b()),
            create_task(downstream()),
        )
        actual = sorted([item async for item in results])
        self.assertEqual([1, 2, 3, 4, 5, 6], actual)

    async def test_processor_fan_in_single_upstream(self):
        # Single upstream still works as before
        results = Collector()
        link = Processor(source=[10, 20, 30], processor=lambda x: x * 2, subscribers=[results])
        await link()
        actual = [item async for item in results]
        self.assertEqual([20, 40, 60], actual)

    async def test_processor_fan_in_with_workers(self):
        # Fan-in combined with concurrent workers
        results = Collector()
        downstream = Processor(processor=lambda x: x + 100, subscribers=[results], workers=2)
        upstream_a = Processor(source=[1, 2], subscribers=[downstream])
        upstream_b = Processor(source=[3, 4], subscribers=[downstream])
        await gather(
            create_task(upstream_a()),
            create_task(upstream_b()),
            create_task(downstream()),
        )
        actual = sorted([item async for item in results])
        self.assertEqual([101, 102, 103, 104], actual)

    # --- Concurrent workers ---

    async def test_processor_workers_produces_correct_results(self):
        results = Collector()
        link = Processor(
            source=list(range(10)),
            processor=lambda x: x * 2,
            subscribers=[results],
            workers=3,
        )
        await link()
        actual = sorted([item async for item in results])
        self.assertEqual(sorted([x * 2 for x in range(10)]), actual)

    async def test_processor_workers_eos_propagates_exactly_once(self):
        # With N workers, downstream should still receive exactly one EOS
        results = Collector()
        link = Processor(source=list(range(5)), processor=lambda x: x, subscribers=[results], workers=4)
        await link()
        actual = sorted([item async for item in results])
        self.assertEqual([0, 1, 2, 3, 4], actual)

    async def test_processor_workers_subscriber_only_mode(self):
        # Multi-worker subscriber-only processor — upstream sends EOS once, workers share it
        results = Collector()
        link = Processor(processor=lambda x: x * 3, subscribers=[results], workers=2)
        for i in range(4):
            await link.push(i)
        await link.push(END_OF_STREAM)
        await link()
        actual = sorted([item async for item in results])
        self.assertEqual(sorted([0, 3, 6, 9]), actual)


class TestProcessorProperties(unittest.TestCase):
    def test_workers_getter(self):
        p = Processor(workers=3)
        self.assertEqual(p.workers, 3)

    def test_workers_setter_enforces_minimum_one(self):
        p = Processor()
        p.workers = 0
        self.assertEqual(p.workers, 1)

    def test_workers_setter_negative_enforces_minimum_one(self):
        p = Processor()
        p.workers = -5
        self.assertEqual(p.workers, 1)

    def test_workers_setter_valid(self):
        p = Processor()
        p.workers = 4
        self.assertEqual(p.workers, 4)

    def test_batch_size_getter(self):
        p = Processor(batch_size=10)
        self.assertEqual(p.batch_size, 10)

    def test_batch_size_setter_enforces_minimum_one(self):
        p = Processor()
        p.batch_size = 0
        self.assertEqual(p.batch_size, 1)

    def test_batch_size_setter_negative_enforces_minimum_one(self):
        p = Processor()
        p.batch_size = -3
        self.assertEqual(p.batch_size, 1)

    def test_batch_size_setter_valid(self):
        p = Processor()
        p.batch_size = 5
        self.assertEqual(p.batch_size, 5)


if __name__ == "__main__":
    unittest.main()
