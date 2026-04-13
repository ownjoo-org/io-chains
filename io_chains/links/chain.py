from asyncio import TaskGroup
from collections.abc import AsyncGenerator, Callable, Iterable
from typing import Any

from io_chains._internal.linkable import Linkable


def _output_of(link: Linkable) -> Linkable:
    """Return the link that publishes output: for a Chain, its last link; otherwise itself."""
    if isinstance(link, Chain):
        return _output_of(link._links[-1])
    return link


def _input_of(link: Linkable) -> Linkable:
    """Return the link that receives pushed input: for a Chain, its first link; otherwise itself."""
    if isinstance(link, Chain):
        return _input_of(link._links[0])
    return link


class Chain(Linkable):
    """
    An ordered sequence of Links that runs as a single unit.

    Chain is the orchestrator: it wires its internal links together,
    manages their concurrent execution, and presents itself as a single
    Linkable to the outside world.

    From the outside a Chain looks like any other Linkable:
      - Accepts input via source= or push() (routed to the first link)
      - Publishes output through its last link's subscribers
      - await chain() runs the whole pipeline; no external gather() needed

    A Chain's elements can be Links, Enrichers, or other Chains.
    """

    def __init__(
        self,
        *args,
        links: list[Linkable],
        source: Callable | Iterable | None = None,
        subscribers: Iterable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._links: list[Linkable] = links

        # Wire each link's output to the next link's input
        for i in range(len(links) - 1):
            _output_of(links[i]).subscribers = [links[i + 1]]

        if source is not None:
            _input_of(links[0]).input = source

        if subscribers is not None:
            _output_of(links[-1]).subscribers = subscribers

    def _register_upstream(self) -> None:
        """Delegate upstream registration to the first link (supports fan-in to a Chain)."""
        first = _input_of(self._links[0])
        if hasattr(first, "_register_upstream"):
            first._register_upstream()

    # --- Linkable interface ---

    @property
    async def input(self) -> AsyncGenerator:
        """Delegates to the first link's input."""
        async for datum in _input_of(self._links[0]).input:
            yield datum

    @input.setter
    def input(self, in_iter) -> None:
        """Route input assignment to the first link."""
        _input_of(self._links[0]).input = in_iter

    async def push(self, datum: Any) -> None:
        """Route pushed data to the first link."""
        await _input_of(self._links[0]).push(datum)

    async def run(self) -> None:
        """Run all internal links concurrently under a TaskGroup.
        Callers just await chain() — no external gather() or create_task() needed."""
        try:
            async with TaskGroup() as tg:
                for link in self._links:
                    tg.create_task(link())
        except ExceptionGroup as eg:
            if len(eg.exceptions) == 1:
                raise eg.exceptions[0]
            raise

    def close(self) -> None:
        """Close all internal links and release references."""
        for link in self._links:
            if hasattr(link, "close"):
                link.close()
        self._links.clear()
        super().close()  # Publisher.close() — clears Chain's own _subscribers

    async def __call__(self) -> None:
        await self.run()
