"""
Unit tests for PersistenceLink.

PersistenceLink is a mid-chain tap: it writes each item to a named store
via AsyncPersistenceManager (upsert by default), then passes the item through
to downstream subscribers unchanged.

Design contract:
  - Calls manager.upsert(store_id, key, item) for every item received
  - Passes every item downstream unchanged
  - key_fn extracts the store key from each item
  - Manages store lifecycle via manager.store_context() (start/flush/close)
  - Does not write EOS to the store
  - Supports on_error: store errors handled without stopping the stream
  - Inherits Link behaviour: metrics, graceful shutdown
"""

import unittest
from collections.abc import Callable
from typing import Any

from oj_persistence.async_manager import AsyncPersistenceManager
from oj_persistence.store.async_base import AsyncAbstractStore

from io_chains.links.chain import Chain
from io_chains.links.collector import Collector
from io_chains.links.persistence_link import PersistenceLink


class TrackingAsyncStore(AsyncAbstractStore):
    """In-memory async store that tracks context-manager lifecycle."""

    def __init__(self):
        self._data: dict[str, Any] = {}
        self.entered = False
        self.exited = False

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *args):
        self.exited = True

    async def create(self, key: str, value: Any) -> None:
        if key in self._data:
            raise KeyError(key)
        self._data[key] = value

    async def read(self, key: str) -> Any:
        return self._data.get(key)

    async def update(self, key: str, value: Any) -> None:
        if key not in self._data:
            raise KeyError(key)
        self._data[key] = value

    async def upsert(self, key: str, value: Any) -> None:
        self._data[key] = value

    async def delete(self, key: str) -> None:
        self._data.pop(key, None)

    async def list(self, predicate: Callable[[Any], bool] | None = None) -> list[Any]:
        values = list(self._data.values())
        return [v for v in values if predicate(v)] if predicate else values


def make_manager(store: AsyncAbstractStore, store_id: str = "s") -> AsyncPersistenceManager:
    AsyncPersistenceManager._instance = None
    pm = AsyncPersistenceManager()
    pm.register(store_id, store)
    return pm


class TestPersistenceLinkInstantiation(unittest.TestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    def test_instantiates(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)
        link = PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]))
        self.assertIsInstance(link, PersistenceLink)

    def test_invalid_operation_raises(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)
        with self.assertRaises(ValueError):
            PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]), operation="replace")


class TestPersistenceLinkPassthrough(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_items_pass_through_to_downstream(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)
        results = Collector()

        chain = Chain(
            source=[{"id": 1}, {"id": 2}, {"id": 3}],
            links=[PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]))],
            subscribers=[results],
        )
        await chain()

        self.assertEqual([item async for item in results], [{"id": 1}, {"id": 2}, {"id": 3}])

    async def test_items_written_to_store(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)

        chain = Chain(
            source=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            links=[PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]))],
        )
        await chain()

        self.assertEqual(await pm.read("s", "1"), {"id": 1, "name": "Alice"})
        self.assertEqual(await pm.read("s", "2"), {"id": 2, "name": "Bob"})

    async def test_eos_not_written_to_store(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)

        chain = Chain(
            source=[{"id": 1}],
            links=[PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]))],
        )
        await chain()

        self.assertEqual(len(store._data), 1)

    async def test_passthrough_value_unchanged_by_store_write(self):
        """Store write is a side effect — original item reaches downstream."""
        store = TrackingAsyncStore()
        pm = make_manager(store)
        results = Collector()

        chain = Chain(
            source=[{"id": 1, "val": 42}],
            links=[PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]))],
            subscribers=[results],
        )
        await chain()

        self.assertEqual([item async for item in results], [{"id": 1, "val": 42}])


class TestPersistenceLinkLifecycle(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_store_context_manager_entered_and_exited(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)

        chain = Chain(
            source=[{"id": 1}],
            links=[PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]))],
        )
        await chain()

        self.assertTrue(store.entered)
        self.assertTrue(store.exited)

    async def test_unknown_store_id_raises_at_run_time(self):
        pm = make_manager(TrackingAsyncStore(), store_id="other")
        link = PersistenceLink(manager=pm, store_id="ghost", key_fn=lambda x: str(x["id"]))
        chain = Chain(source=[{"id": 1}], links=[link])
        with self.assertRaises(KeyError):
            await chain()


class TestPersistenceLinkOperations(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_create_operation(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)

        chain = Chain(
            source=[{"id": 1, "name": "Alice"}],
            links=[PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]), operation="create")],
        )
        await chain()

        self.assertEqual(await pm.read("s", "1"), {"id": 1, "name": "Alice"})

    async def test_update_operation(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)
        await store.upsert("1", {"id": 1, "name": "Old"})

        chain = Chain(
            source=[{"id": 1, "name": "New"}],
            links=[PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]), operation="update")],
        )
        await chain()

        self.assertEqual(await pm.read("s", "1"), {"id": 1, "name": "New"})


class TestPersistenceLinkErrorHandling(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_store_error_propagates_without_on_error(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)
        await store.upsert("1", {"id": 1})  # pre-seed so create() raises

        chain = Chain(
            source=[{"id": 1}],
            links=[PersistenceLink(manager=pm, store_id="s", key_fn=lambda x: str(x["id"]), operation="create")],
        )
        with self.assertRaises(KeyError):
            await chain()

    async def test_on_error_recovers_from_store_error(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)
        await store.upsert("1", {"id": 1})  # pre-seed so create() raises on item 1
        errors = []
        results = Collector()

        chain = Chain(
            source=[{"id": 1}, {"id": 2}],
            links=[
                PersistenceLink(
                    manager=pm,
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
        # both items still pass through
        self.assertEqual([item async for item in results], [{"id": 1}, {"id": 2}])
        # item 2 was written successfully
        self.assertEqual(await pm.read("s", "2"), {"id": 2})


class TestPersistenceLinkMetrics(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        AsyncPersistenceManager._instance = None

    def tearDown(self):
        AsyncPersistenceManager._instance = None

    async def test_metrics_tracked(self):
        store = TrackingAsyncStore()
        pm = make_manager(store)
        captured = []

        chain = Chain(
            source=[{"id": 1}, {"id": 2}, {"id": 3}],
            links=[
                PersistenceLink(
                    manager=pm,
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
