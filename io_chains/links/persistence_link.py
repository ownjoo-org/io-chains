from collections.abc import AsyncIterable, Callable, Iterable
from inspect import isawaitable
from typing import Any

from oj_persistence import Manager

from io_chains._internal.link import Link
from io_chains._internal.sentinel import END_OF_STREAM, EndOfStream

_OPERATIONS = {"upsert", "create", "update"}


class PersistenceLink(Link):
    """
    A mid-chain tap: writes each item to a named table via
    ``oj_persistence.Manager`` as a side effect, then passes the item through
    to downstream subscribers unchanged.

    The target table must already be registered on the Manager before the link
    runs (typically by the pipeline owner at setup time). The link itself does
    not create, truncate, or drop the table — it's purely a writer.

    Parameters
    ----------
    manager:    ``Manager`` instance. Injected for testability and to support
                per-pipeline / per-tenant Managers.
    store_id:   Name under which the target table is registered on the Manager.
    key_fn:     Callable that extracts the store key (str) from each item.
    operation:  'upsert' (default) | 'create' | 'update'
    """

    def __init__(
        self,
        *args,
        manager: Manager,
        store_id: str,
        key_fn: Callable[[Any], str],
        operation: str = "upsert",
        source: Callable | Iterable | None = None,
        **kwargs,
    ) -> None:
        if operation not in _OPERATIONS:
            raise ValueError(f"operation must be one of {_OPERATIONS}, got {operation!r}")
        super().__init__(*args, **kwargs)
        self._manager = manager
        self._store_id = store_id
        self._key_fn = key_fn
        self._operation = operation
        self._input: AsyncIterable | Callable | Iterable | None = None
        self.input = source

    @property
    async def input(self):
        if self._input is None:
            return
        src = self._input() if callable(self._input) else self._input
        if hasattr(src, "__aiter__"):
            async for each in src:
                yield each
        else:
            for each in src:
                yield each

    @input.setter
    def input(self, in_obj) -> None:
        self._input = in_obj

    async def _fill_queue_from_input(self) -> None:
        if self._input is not None:
            async for datum in self.input:
                await self.push(datum)
            await self.push(END_OF_STREAM)

    async def run(self) -> None:
        self._reset_metrics()
        from asyncio import TaskGroup
        try:
            async with TaskGroup() as tg:
                tg.create_task(self._fill_queue_from_input())
                tg.create_task(self._process_loop())
        except ExceptionGroup as eg:
            if len(eg.exceptions) == 1:
                raise eg.exceptions[0]
            raise
        await self._emit_metrics()

    async def _process_loop(self) -> None:
        while True:
            datum = await self._queue.get()
            if isinstance(datum, EndOfStream):
                await self.publish(END_OF_STREAM)
                break
            try:
                await self._write(self._key_fn(datum), datum)
            except Exception as e:
                self._items_errored += 1
                if self._on_error is not None:
                    handler = self._on_error(e, datum)
                    if isawaitable(handler):
                        await handler
                else:
                    raise
            await self.publish(datum)

    async def _write(self, key: str, value: Any) -> None:
        if self._operation == "upsert":
            await self._manager.aupsert(self._store_id, key, value)
        elif self._operation == "create":
            await self._manager.acreate(self._store_id, key, value)
        else:  # update
            await self._manager.aupdate(self._store_id, key, value)
