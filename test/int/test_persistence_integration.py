"""End-to-end: chain → PersistenceLink → oj-persistence Sqlite → read back.

Covers the scenario that fast-etl's staged pipeline hits: stage-1 writes via
PersistenceLink into a SQLite-backed table, stage-2 reads back via the Manager.
If this passes, io-chains + persistence agree on the write-then-read contract.
If fast-etl still fails in production, the bug is above io-chains (fast-etl
glue code).
"""

from __future__ import annotations

import unittest

from oj_persistence import InMemory, Manager, Sqlite

from io_chains.links.chain import Chain
from io_chains.links.persistence_link import PersistenceLink


class TestPersistenceLinkWriteThenRead(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.pm = Manager()

    async def asyncTearDown(self):
        await self.pm.aclose()

    async def _assert_roundtrip(self, spec) -> None:
        """Register a table with the given backend, write 20 items through a
        PersistenceLink chain, then verify every item is readable by name and
        by list_page."""
        await self.pm.aregister('users', spec)

        source = [{'id': i, 'name': f'user-{i}'} for i in range(20)]
        chain = Chain(
            source=source,
            links=[PersistenceLink(
                manager=self.pm,
                store_id='users',
                key_fn=lambda v: str(v['id']),
            )],
        )
        await chain()

        # Individual reads
        for i in range(20):
            row = await self.pm.aread('users', str(i))
            self.assertIsNotNone(row)
            self.assertEqual(row['id'], i)

        # Paginated read (stage-2-style)
        rows = await self.pm.alist_page('users', 0, 100)
        self.assertEqual(len(rows), 20)

    async def test_write_then_read_in_memory(self):
        await self._assert_roundtrip(InMemory())

    # Note: we intentionally don't test Sqlite(path=':memory:') in the
    # integration suite. Each sqlite3.connect(':memory:') creates a separate
    # private database, so the writer connection and the reader-pool
    # connections wouldn't share state — it's not a real test of the
    # write-then-read contract. Use a file-backed path instead.

    async def test_write_then_read_sqlite_file(self):
        import tempfile, os, shutil
        tmp = tempfile.mkdtemp()
        try:
            await self._assert_roundtrip(Sqlite(path=os.path.join(tmp, 'db.sqlite')))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


class TestStagedWriteThenRead(unittest.IsolatedAsyncioTestCase):
    """Closer replica of fast-etl's staged pipeline:

      stage 1: three chains each write to their own table (shared sqlite file)
      stage 2: read every row from each table via the same Manager
    """

    async def asyncSetUp(self):
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db = f'{self._tmpdir.name}/db.sqlite'
        self.pm = Manager()
        await self.pm.aregister('characters', Sqlite(path=self.db))
        await self.pm.aregister('locations', Sqlite(path=self.db))
        await self.pm.aregister('episodes', Sqlite(path=self.db))

    async def asyncTearDown(self):
        await self.pm.aclose()
        self._tmpdir.cleanup()

    async def test_stage1_writes_stage2_reads(self):
        # Stage 1 — three PersistenceLink chains running concurrently.
        import asyncio
        sources = {
            'characters': [{'id': i, 'name': f'char-{i}'} for i in range(20)],
            'locations': [{'id': i, 'name': f'loc-{i}'} for i in range(20)],
            'episodes': [{'id': i, 'name': f'ep-{i}'} for i in range(20)],
        }
        chains = [
            Chain(
                source=data,
                links=[PersistenceLink(
                    manager=self.pm,
                    store_id=table,
                    key_fn=lambda v: str(v['id']),
                )],
            )
            for table, data in sources.items()
        ]
        await asyncio.gather(*(c() for c in chains))

        # Stage 2 — read back every row via Manager.alist_page.
        for table in ('characters', 'locations', 'episodes'):
            rows = await self.pm.alist_page(table, 0, 100)
            self.assertEqual(
                len(rows), 20,
                f'{table}: expected 20, got {len(rows)}',
            )

    async def test_poll_during_stage1_then_stage2_read(self):
        """Mimic the UI's /results polling hitting the Manager while stage-1 is running."""
        import asyncio
        source = [{'id': i, 'name': f'char-{i}'} for i in range(30)]

        # Poller on a separate table (characters' sibling) — like /results poll.
        stop = asyncio.Event()
        poll_counts = []

        async def poller() -> None:
            while not stop.is_set():
                try:
                    poll_counts.append(len(await self.pm.alist_page('locations', 0, 100)))
                except Exception:
                    poll_counts.append(-1)
                await asyncio.sleep(0.005)

        poll_task = asyncio.create_task(poller())

        # Stage 1 writes characters.
        chain = Chain(
            source=source,
            links=[PersistenceLink(
                manager=self.pm,
                store_id='characters',
                key_fn=lambda v: str(v['id']),
            )],
        )
        await chain()

        stop.set()
        await poll_task

        # Stage 2 read — this is what fast-etl's StoreReadLink does.
        rows = await self.pm.alist_page('characters', 0, 100)
        self.assertEqual(
            len(rows), 30,
            f'stage-2 read saw {len(rows)} rows, expected 30 (poll counts during stage 1: {poll_counts[:10]}…)',
        )


if __name__ == '__main__':
    unittest.main()
