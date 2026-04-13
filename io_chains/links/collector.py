from asyncio import Queue, QueueEmpty
from logging import getLogger
from typing import Any

from io_chains._internal.sentinel import EndOfStream
from io_chains._internal.subscriber import Subscriber

logger = getLogger(__name__)


class Collector(Subscriber):
    """
    Subscriber that buffers items in a queue for iteration.

    Supports both async and sync iteration:
        async for item in collector: ...   (preferred; works inside a running event loop)
        for item in collector: ...         (works after the pipeline has completed)

    When subscribed via subscribe(channel=...) from multiple upstreams, the Collector
    automatically counts how many channels are wired (via _register_upstream) and waits
    for all of them to send EndOfStream before stopping iteration.
    """

    def __init__(self, maxsize: int = 0):
        self._queue: Queue = Queue(maxsize=maxsize)
        self._upstream_count: int = 0  # 0 = single-upstream legacy mode
        self._eos_received: int = 0

    def _register_upstream(self) -> None:
        """Called by ChannelSubscriber when wiring to register one upstream channel."""
        self._upstream_count += 1

    async def push(self, datum: Any) -> None:
        if isinstance(datum, EndOfStream):
            if self._upstream_count == 0:
                # Legacy single-upstream mode: first EOS stops the collector
                await self._queue.put(datum)
            else:
                # asyncio is single-threaded; no yield between increment and check
                self._eos_received += 1
                if self._eos_received >= self._upstream_count:
                    await self._queue.put(datum)
        else:
            await self._queue.put(datum)

    def __aiter__(self):
        return self

    async def __anext__(self) -> Any:
        item = await self._queue.get()
        if isinstance(item, EndOfStream):
            raise StopAsyncIteration
        return item

    def close(self) -> None:
        """Drain and discard all buffered items.

        Call after the pipeline has completed to free memory held by unconsumed
        output.  Items already yielded by iteration are unaffected.
        """
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except QueueEmpty:
                break

    def __iter__(self):
        """Drain buffered items synchronously. Only use after the pipeline has completed."""
        while True:
            try:
                item = self._queue.get_nowait()
            except QueueEmpty:
                break
            if isinstance(item, EndOfStream):
                break
            yield item
