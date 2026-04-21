"""
Unit tests for PersistenceLink against oj_persistence.Manager (v2 API).

PersistenceLink is a mid-chain tap: it writes each item to a named table
via the Manager (upsert by default), then passes the item through to
downstream subscribers unchanged.

Design contract:
  - Calls manager.aupsert/acreate/aupdate(store_id, key, item) per item
  - Passes every item downstream unchanged
  - key_fn extracts the table key from each item
  - Does not write EOS to the store
  - Supports on_error: store errors handled without stopping the stream
  - Inherits Link behaviour: metrics, graceful shutdown
"""

import unittest

from oj_persistence import InMemory, Manager

from io_chains.links.chain import Chain
from io_chains.links.collector import Collector
from io_chains.links.persistence_link import PersistenceLink


def _fresh_manager(store_id: str = "s") -> Manager:
    pm = Manager()
    pm.register(store_id, InMemory())
    return pm


class TestPersistenceLinkInstantiation(unittest.TestCase):
    def test_instantiates(self):
        pm = _fresh_manager()
        link = PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]))
        self.assertIsInstance(link, PersistenceLink)
        pm.close()

    def test_invalid_operation_raises(self):
        pm = _fresh_manager()
        with self.assertRaises(ValueError):
            PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]), operation="replace")
        pm.close()


class TestPersistenceLinkPassthrough(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pm = Manager()
        await self.pm.aregister("s", InMemory())

    async def asyncTearDown(self):
        await self.pm.aclose()

    async def test_items_pass_through_to_downstream(self):
        results = Collector()
        chain = Chain(
            source=[{"id": 1}, {"id": 2}, {"id": 3}],
            links=[PersistenceLink(manager=self.pm, store_id="s", key_fn=lambda x: str(x["id"]))],
            subscribers=[results],
        )
        await chain()
        self.assertEqual([item async for item in results], [{"id": 1}, {"id": 2}, {"id": 3}])

    async def test_items_written_to_store(self):
        chain = Chain(
            source=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            links=[PersistenceLink(manager=self.pm, store_id="s", key_fn=lambda x: str(x["id"]))],
        )
        await chain()
        self.assertEqual(await self.pm.aread("s", "1"), {"id": 1, "name": "Alice"})
        self.assertEqual(await self.pm.aread("s", "2"), {"id": 2, "name": "Bob"})

    async def test_eos_not_written_to_store(self):
        chain = Chain(
            source=[{"id": 1}],
            links=[PersistenceLink(manager=self.pm, store_id="s", key_fn=lambda x: str(x["id"]))],
        )
        await chain()
        self.assertEqual(len(await self.pm.alist("s")), 1)

    async def test_passthrough_value_unchanged_by_store_write(self):
        results = Collector()
        chain = Chain(
            source=[{"id": 1, "val": 42}],
            links=[PersistenceLink(manager=self.pm, store_id="s", key_fn=lambda x: str(x["id"]))],
            subscribers=[results],
        )
        await chain()
        self.assertEqual([item async for item in results], [{"id": 1, "val": 42}])


class TestPersistenceLinkLifecycle(unittest.IsolatedAsyncioTestCase):
    """In v2 the Manager owns backend lifecycle — the link no longer wraps a
    store_context(). These tests verify the contract from the link's perspective:
    no special setup/teardown on the store is needed."""

    async def asyncSetUp(self):
        self.pm = Manager()
        await self.pm.aregister("other", InMemory())

    async def asyncTearDown(self):
        await self.pm.aclose()

    async def test_unknown_store_id_raises_at_run_time(self):
        from oj_persistence import TableNotRegistered
        link = PersistenceLink(manager=self.pm, store_id="ghost", key_fn=lambda x: str(x["id"]))
        chain = Chain(source=[{"id": 1}], links=[link])
        with self.assertRaises(TableNotRegistered):
            await chain()


class TestPersistenceLinkOperations(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pm = Manager()
        await self.pm.aregister("s", InMemory())

    async def asyncTearDown(self):
        await self.pm.aclose()

    async def test_create_operation(self):
        chain = Chain(
            source=[{"id": 1, "name": "Alice"}],
            links=[PersistenceLink(manager=self.pm, store_id="s", key_fn=lambda x: str(x["id"]), operation="create")],
        )
        await chain()
        self.assertEqual(await self.pm.aread("s", "1"), {"id": 1, "name": "Alice"})

    async def test_update_operation(self):
        await self.pm.aupsert("s", "1", {"id": 1, "name": "Old"})
        chain = Chain(
            source=[{"id": 1, "name": "New"}],
            links=[PersistenceLink(manager=self.pm, store_id="s", key_fn=lambda x: str(x["id"]), operation="update")],
        )
        await chain()
        self.assertEqual(await self.pm.aread("s", "1"), {"id": 1, "name": "New"})


class TestPersistenceLinkErrorHandling(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pm = Manager()
        await self.pm.aregister("s", InMemory())

    async def asyncTearDown(self):
        await self.pm.aclose()

    async def test_store_error_propagates_without_on_error(self):
        # pre-seed so create() raises
        await self.pm.aupsert("s", "1", {"id": 1})
        chain = Chain(
            source=[{"id": 1}],
            links=[PersistenceLink(manager=self.pm, store_id="s", key_fn=lambda x: str(x["id"]), operation="create")],
        )
        with self.assertRaises(KeyError):
            await chain()

    async def test_on_error_recovers_from_store_error(self):
        # pre-seed so create() raises on item 1
        await self.pm.aupsert("s", "1", {"id": 1})
        errors = []
        results = Collector()
        chain = Chain(
            source=[{"id": 1}, {"id": 2}],
            links=[
                PersistenceLink(
                    manager=self.pm,
                    store_id="s",
                    key_fn=lambda x: str(x["id"]),
                    operation="create",
                    on_error=lambda e, item: errors.append(item),
                )
            ],
            subscribers=[results],
        )
        await chain()
        self.assertEqual(errors, [{"id": 1}])
        self.assertEqual([item async for item in results], [{"id": 1}, {"id": 2}])
        self.assertEqual(await self.pm.aread("s", "2"), {"id": 2})


class TestPersistenceLinkMetrics(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pm = Manager()
        await self.pm.aregister("s", InMemory())

    async def asyncTearDown(self):
        await self.pm.aclose()

    async def test_metrics_tracked(self):
        captured = []
        chain = Chain(
            source=[{"id": 1}, {"id": 2}, {"id": 3}],
            links=[
                PersistenceLink(
                    manager=self.pm,
                    store_id="s",
                    key_fn=lambda x: str(x["id"]),
                    on_metrics=lambda m: captured.append(m),
                )
            ],
        )
        await chain()
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].items_in, 3)
        self.assertEqual(captured[0].items_out, 3)


if __name__ == "__main__":
    unittest.main()
