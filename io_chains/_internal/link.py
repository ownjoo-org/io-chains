import asyncio
import time
from abc import abstractmethod
from asyncio import Lock, Queue
from collections.abc import Callable
from logging import getLogger
from typing import Any

from io_chains._internal.linkable import Linkable
from io_chains._internal.metrics import LinkMetrics
from io_chains._internal.sentinel import END_OF_STREAM, EndOfStream

logger = getLogger(__name__)


class Link(Linkable):
    """
    Linkable with an internal asyncio Queue and upstream EOS counting.

    Shared machinery for any link that buffers incoming data in a queue
    and waits for all registered upstreams to signal end-of-stream before
    passing EOS downstream.

    Subclasses must implement:
      - input property (from Linkable)
      - run() — consume from self._queue and publish results
    """

    def __init__(self, *args, queue_size: int = 0, on_error: Callable | None = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._queue: Queue = Queue(maxsize=queue_size)
        self._on_error: Callable | None = on_error
        self._upstream_count: int = 0
        self._eos_received: int = 0

        # Metrics counters — protected by the asyncio single-thread guarantee
        # for items_in/items_skipped/items_errored; items_out uses a Lock because
        # multiple workers may call publish() concurrently.
        self._items_in: int = 0
        self._items_out: int = 0
        self._items_skipped: int = 0
        self._items_errored: int = 0
        self._metrics_lock: Lock = Lock()
        self._start_time: float = 0.0

    def _register_upstream(self) -> None:
        self._upstream_count += 1

    def _eos_queue_count(self) -> int:
        """Number of EOS signals to enqueue when all upstreams are done.

        Default is 1 (single consumer loop). Override when multiple workers
        each need their own EOS signal to shut down.
        """
        return 1

    async def push(self, datum: Any) -> None:
        if isinstance(datum, EndOfStream):
            self._eos_received += 1
            if self._eos_received < self._upstream_count:
                return  # still waiting for other upstreams
            for _ in range(self._eos_queue_count()):
                await self._queue.put(datum)
        else:
            self._items_in += 1
            await self._queue.put(datum)

    async def publish(self, datum: Any) -> None:
        if not isinstance(datum, EndOfStream):
            async with self._metrics_lock:
                self._items_out += 1
        await super().publish(datum)

    def _reset_metrics(self) -> None:
        self._items_in = 0
        self._items_out = 0
        self._items_skipped = 0
        self._items_errored = 0
        self._start_time = time.monotonic()

    async def _emit_metrics(self) -> None:
        elapsed = time.monotonic() - self._start_time
        metrics = LinkMetrics(
            name=self.name,
            items_in=self._items_in,
            items_out=self._items_out,
            items_skipped=self._items_skipped,
            items_errored=self._items_errored,
            elapsed_seconds=elapsed,
        )
        logger.info(
            "link %r completed: in=%d out=%d skipped=%d errored=%d elapsed=%.4fs",
            self.name,
            metrics.items_in,
            metrics.items_out,
            metrics.items_skipped,
            metrics.items_errored,
            metrics.elapsed_seconds,
            extra={
                "link_name": self.name,
                "items_in": metrics.items_in,
                "items_out": metrics.items_out,
                "items_skipped": metrics.items_skipped,
                "items_errored": metrics.items_errored,
                "elapsed_seconds": metrics.elapsed_seconds,
            },
        )
        if self._on_metrics is not None:
            result = self._on_metrics(metrics)
            if hasattr(result, "__await__"):
                await result

    def close(self) -> None:
        """Drain the internal queue and release subscriber references.

        Call once the pipeline has completed.  The queue should already be
        empty after a normal run; this is a safety drain for cancelled or
        failed pipelines where items may remain.
        """
        super().close()  # Publisher.close() — clears _subscribers
        q = self._queue
        while not q.empty():
            try:
                q.get_nowait()
            except Exception:  # noqa: BLE001
                break

    @abstractmethod
    async def run(self) -> None:
        pass

    async def __call__(self) -> None:
        try:
            await self.run()
        except asyncio.CancelledError:
            # Schedule EOS on the event loop independently of this (cancelled) task
            # so downstream links receive end-of-stream and don't hang.
            asyncio.get_running_loop().create_task(self.publish(END_OF_STREAM))
            raise
