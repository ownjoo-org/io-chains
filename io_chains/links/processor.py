from asyncio import Lock, TaskGroup
from collections.abc import AsyncGenerator, AsyncIterable, Callable, Iterable
from inspect import isawaitable, isgenerator
from logging import getLogger
from typing import Any

from io_chains._internal.link import Link
from io_chains._internal.sentinel import END_OF_STREAM, EndOfStream, Skip

logger = getLogger(__name__)


class Processor(Link):
    def __init__(
        self,
        *args,
        source: Callable | Iterable | None = None,
        processor: Callable | None = None,
        workers: int = 1,
        batch_size: int = 1,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._input: AsyncIterable | Callable | Iterable | None = None
        self.input = source

        self._processor: Callable | None = processor
        self._workers: int = 1
        self._active_workers: int = 1
        self._batch_size: int = 1
        self.workers = workers
        self.batch_size = batch_size

    @property
    def workers(self) -> int:
        return self._workers

    @workers.setter
    def workers(self, value: int) -> None:
        self._workers = max(1, value)

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @batch_size.setter
    def batch_size(self, value: int) -> None:
        self._batch_size = max(1, value)

    def _eos_queue_count(self) -> int:
        return self._workers

    @property
    async def input(self) -> AsyncGenerator:
        if self._input is None:
            return
        source = self._input() if callable(self._input) else self._input
        if hasattr(source, "__aiter__"):
            async for each in source:
                yield each
        else:
            for each in source:
                yield each

    @input.setter
    def input(self, in_obj: AsyncIterable | Callable | Iterable | None) -> None:
        if in_obj is not None and not isinstance(in_obj, (AsyncIterable, Callable, Iterable)):
            raise TypeError(f"source must be Callable or Iterable, got {type(in_obj)}")
        self._input = in_obj

    async def _fill_queue_from_input(self) -> None:
        if self._input is not None:
            try:
                async for datum in self.input:
                    await self.push(datum)
            except Exception as e:
                self._items_errored += 1
                if self._on_error is not None:
                    result = self._on_error(e, None)
                    if hasattr(result, "__await__"):
                        result = await result
                    # Skip or None → suppress; anything else → push to queue
                    if result is not None and not isinstance(result, Skip):
                        await self.push(result)
                else:
                    raise
            await self.push(END_OF_STREAM)
        # else: subscriber-only mode — EOS arrives via push() from upstream

    async def _collect_batch(self) -> tuple[list, bool]:
        """Collect up to batch_size items. Returns (batch, eos_received)."""
        batch = []
        while len(batch) < self._batch_size:
            datum = await self._queue.get()
            if isinstance(datum, EndOfStream):
                return batch, True
            batch.append(datum)
        return batch, False

    async def _process_and_publish(self, datum: Any) -> None:
        """Process datum (or batch list) and publish result(s)."""
        if self._processor:
            try:
                result = self._processor(datum)
                if isawaitable(result):
                    result = await result
            except Exception as e:
                self._items_errored += 1
                if self._on_error is not None:
                    result = self._on_error(e, datum)
                    if isawaitable(result):
                        result = await result
                else:
                    raise

            if isinstance(result, Skip):
                self._items_skipped += 1
                return
            elif isgenerator(result):
                for item in result:
                    await self.publish(item)
                return
            elif hasattr(result, "__aiter__"):
                async for item in result:
                    await self.publish(item)
                return
            datum = result
        elif self._batch_size > 1 and isinstance(datum, list):
            # No processor in batch mode: publish each item individually
            for item in datum:
                await self.publish(item)
            return

        await self.publish(datum)

    async def _handle_eos(self, lock: Lock) -> None:
        async with lock:
            self._active_workers -= 1
            if self._active_workers == 0:
                await self.publish(END_OF_STREAM)

    async def _worker_batch(self, lock: Lock) -> None:
        while True:
            batch, eos_received = await self._collect_batch()
            if batch:
                await self._process_and_publish(batch)
            if eos_received:
                await self._handle_eos(lock)
                break

    async def _worker_single(self, lock: Lock) -> None:
        while True:
            datum = await self._queue.get()
            if isinstance(datum, EndOfStream):
                await self._handle_eos(lock)
                break
            await self._process_and_publish(datum)

    def close(self) -> None:
        """Release input and subscriber references after the pipeline has completed."""
        super().close()  # Link.close() — drains queue + clears _subscribers
        self._input = None
        self._processor = None

    async def run(self) -> None:
        self._active_workers = self._workers
        self._eos_received = 0
        self._reset_metrics()
        lock = Lock()
        worker = self._worker_batch if self._batch_size > 1 else self._worker_single
        try:
            async with TaskGroup() as tg:
                tg.create_task(self._fill_queue_from_input())
                for _ in range(self._workers):
                    tg.create_task(worker(lock))
        except ExceptionGroup as eg:
            if len(eg.exceptions) == 1:
                raise eg.exceptions[0]
            raise
        await self._emit_metrics()
