"""
Unit tests for ErrorEnvelope sentinel and Publisher.error_subscribers / publish_error.
"""
import unittest

from io_chains._internal.sentinel import END_OF_STREAM, ErrorEnvelope
from io_chains._internal.publisher import Publisher
from io_chains.links.collector import Collector


# ---------------------------------------------------------------------------
# ErrorEnvelope
# ---------------------------------------------------------------------------


class TestErrorEnvelope(unittest.TestCase):
    def test_stores_fields(self):
        exc = ValueError("boom")
        env = ErrorEnvelope(datum={"id": 1}, exc=exc, link_name="my-link", retry_count=2)
        self.assertEqual(env.datum, {"id": 1})
        self.assertIs(env.exc, exc)
        self.assertEqual(env.link_name, "my-link")
        self.assertEqual(env.retry_count, 2)
        self.assertFalse(env.handled)

    def test_defaults(self):
        env = ErrorEnvelope(datum=42, exc=RuntimeError("x"))
        self.assertEqual(env.link_name, "")
        self.assertEqual(env.retry_count, 0)
        self.assertFalse(env.handled)

    def test_repr_is_readable(self):
        env = ErrorEnvelope(datum=1, exc=KeyError("k"), link_name="stage", retry_count=1)
        r = repr(env)
        self.assertIn("stage", r)
        self.assertIn("retry=1", r)
        self.assertIn("handled=False", r)

    def test_handled_flag_is_mutable(self):
        env = ErrorEnvelope(datum=None, exc=Exception())
        env.handled = True
        self.assertTrue(env.handled)

    def test_distinct_from_end_of_stream(self):
        env = ErrorEnvelope(datum=None, exc=Exception())
        self.assertIsNot(env, END_OF_STREAM)
        self.assertNotIsInstance(env, type(END_OF_STREAM))


# ---------------------------------------------------------------------------
# Publisher.error_subscribers / publish_error
# ---------------------------------------------------------------------------


class TestPublisherErrorSubscribers(unittest.IsolatedAsyncioTestCase):
    def test_error_subscribers_empty_by_default(self):
        p = Publisher()
        self.assertEqual(p.error_subscribers, [])

    def test_set_single_error_subscriber(self):
        p = Publisher()
        c = Collector()
        p.error_subscribers = c
        self.assertEqual(len(p.error_subscribers), 1)

    def test_set_list_of_error_subscribers(self):
        p = Publisher()
        subs = [Collector(), Collector()]
        p.error_subscribers = subs
        self.assertEqual(len(p.error_subscribers), 2)

    def test_rejects_non_subscriber(self):
        p = Publisher()
        with self.assertRaises(TypeError):
            p.error_subscribers = [lambda x: x]

    def test_close_clears_error_subscribers(self):
        p = Publisher()
        p.error_subscribers = Collector()
        p.close()
        self.assertEqual(p.error_subscribers, [])

    async def test_publish_error_returns_false_when_no_subscribers(self):
        p = Publisher()
        env = ErrorEnvelope(datum=1, exc=ValueError())
        result = await p.publish_error(env)
        self.assertFalse(result)
        self.assertFalse(env.handled)

    async def test_publish_error_routes_to_subscriber_and_sets_handled(self):
        p = Publisher()
        collector = Collector()
        p.error_subscribers = collector
        env = ErrorEnvelope(datum=42, exc=RuntimeError("x"))
        result = await p.publish_error(env)
        self.assertTrue(result)
        self.assertTrue(env.handled)
        # Collector received the envelope (send EOS so we can drain)
        await collector.push(END_OF_STREAM)
        items = [item async for item in collector]
        self.assertEqual(len(items), 1)
        self.assertIs(items[0], env)

    async def test_publish_error_fans_out_to_multiple_subscribers(self):
        p = Publisher()
        c1, c2 = Collector(), Collector()
        p.error_subscribers = [c1, c2]
        env = ErrorEnvelope(datum="x", exc=Exception())
        await p.publish_error(env)
        self.assertTrue(env.handled)
        for c in (c1, c2):
            await c.push(END_OF_STREAM)
            items = [item async for item in c]
            self.assertEqual(items, [env])

    async def test_publish_error_does_not_affect_normal_subscribers(self):
        """Error routing must not bleed into the regular subscriber list."""
        p = Publisher()
        normal = Collector()
        error_sink = Collector()
        p.subscribers = normal
        p.error_subscribers = error_sink
        env = ErrorEnvelope(datum=1, exc=Exception())
        await p.publish_error(env)
        await normal.push(END_OF_STREAM)
        normal_items = [item async for item in normal]
        self.assertEqual(normal_items, [])  # no bleed-over


if __name__ == "__main__":
    unittest.main()
