import asyncio
import time
import tracemalloc
from abc import abstractmethod
from asyncio import Queue
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
        self._on_error: Callable | None = None
        self._upstream_count: int = 0
        self._eos_received: int = 0

        self._items_in: int = 0
        self._items_skipped: int = 0
        self._items_errored: int = 0
        self._queue_depth_max: int = 0
        self._mem_baseline: int = 0
        self._start_time: float = 0.0

        self.on_error = on_error

    @property
    def on_error(self) -> Callable | None:
        return self._on_error

    @on_error.setter
    def on_error(self, value: Callable | None) -> None:
        if value is not None and not callable(value):
            raise TypeError(f"on_error must be callable or None, got {type(value)}")
        self._on_error = value

    @property
    def items_in(self) -> int:
        return self._items_in

    @property
    def items_skipped(self) -> int:
        return self._items_skipped

    @property
    def items_errored(self) -> int:
        return self._items_errored

    @property
    def upstream_count(self) -> int:
        return self._upstream_count

    @property
    def queue_depth_max(self) -> int:
        return self._queue_depth_max

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
            depth = self._queue.qsize()
            if depth > self._queue_depth_max:
                self._queue_depth_max = depth

    def _reset_metrics(self) -> None:
        self._items_in = 0
        self._items_out = 0
        self._items_skipped = 0
        self._items_errored = 0
        self._queue_depth_max = 0
        self._mem_baseline = tracemalloc.get_traced_memory()[1] if tracemalloc.is_tracing() else 0
        self._start_time = time.monotonic()

    async def _emit_metrics(self) -> None:
        elapsed = time.monotonic() - self._start_time
        mem_peak = tracemalloc.get_traced_memory()[1] if tracemalloc.is_tracing() else 0
        metrics = LinkMetrics(
            name=self.name,
            items_in=self._items_in,
            items_out=self._items_out,
            items_skipped=self._items_skipped,
            items_errored=self._items_errored,
            elapsed_seconds=elapsed,
            memory_peak_bytes=max(0, mem_peak - self._mem_baseline),
            time_per_item_seconds=elapsed / self._items_in if self._items_in > 0 else 0.0,
            subscribed_count=self._upstream_count,
            subscriber_count=self.subscriber_count,
            queue_depth_max=self._queue_depth_max,
            throughput_items_per_sec=self.items_out / elapsed if elapsed > 0 else 0.0,
        )
        logger.info(
            "link %r completed: in=%d out=%d skipped=%d errored=%d elapsed=%.4fs "
            "mem=%dB per_item=%.6fs subscribed=%d subscribers=%d queue_max=%d tput=%.1f/s",
            self.name,
            metrics.items_in,
            metrics.items_out,
            metrics.items_skipped,
            metrics.items_errored,
            metrics.elapsed_seconds,
            metrics.memory_peak_bytes,
            metrics.time_per_item_seconds,
            metrics.subscribed_count,
            metrics.subscriber_count,
            metrics.queue_depth_max,
            metrics.throughput_items_per_sec,
            extra={
                "link_name": self.name,
                "items_in": metrics.items_in,
                "items_out": metrics.items_out,
                "items_skipped": metrics.items_skipped,
                "items_errored": metrics.items_errored,
                "elapsed_seconds": metrics.elapsed_seconds,
                "memory_peak_bytes": metrics.memory_peak_bytes,
                "time_per_item_seconds": metrics.time_per_item_seconds,
                "subscribed_count": metrics.subscribed_count,
                "subscriber_count": metrics.subscriber_count,
                "queue_depth_max": metrics.queue_depth_max,
                "throughput_items_per_sec": metrics.throughput_items_per_sec,
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
