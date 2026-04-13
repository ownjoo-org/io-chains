from asyncio import TaskGroup
from collections.abc import Callable, Iterable
from typing import Any

from io_chains._internal.subscriber import Subscriber


class Publisher:
    def __init__(
        self,
        *args,
        name: str = "",
        subscribers: "Subscriber | Iterable[Subscriber] | None" = None,
        on_metrics: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.name: str = name
        self._on_metrics: Callable | None = on_metrics
        self._subscribers: list[Subscriber] = []
        self.subscribers = subscribers

    @property
    def subscribers(self) -> list[Subscriber]:
        return self._subscribers

    @subscribers.setter
    def subscribers(self, subscribers: "Subscriber | Iterable[Subscriber] | None") -> None:
        if not subscribers:
            return
        if isinstance(subscribers, Subscriber):
            self._subscribers.append(subscribers)
            if hasattr(subscribers, "_register_upstream"):
                subscribers._register_upstream()
        elif isinstance(subscribers, Iterable):
            for subscriber in subscribers:
                if not isinstance(subscriber, Subscriber):
                    raise TypeError(f"each subscriber must be a Subscriber, got {type(subscriber)}")
                self._subscribers.append(subscriber)
                if hasattr(subscriber, "_register_upstream"):
                    subscriber._register_upstream()
        else:
            raise TypeError("subscribers must be a Subscriber or Iterable[Subscriber]")

    def subscribe(self, subscriber: Subscriber, channel: str | None = None) -> None:
        """Wire a subscriber, optionally tagging each item with a channel label.

        subscribe(sub)               — equivalent to appending sub directly
        subscribe(sub, channel='x')  — wraps each item in Envelope(data, channel='x')
        """
        if channel is not None:
            from io_chains._internal.channel_subscriber import ChannelSubscriber

            # ChannelSubscriber.__init__ calls subscriber._register_upstream() if present
            subscriber = ChannelSubscriber(subscriber=subscriber, channel=channel)
        else:
            if hasattr(subscriber, "_register_upstream"):
                subscriber._register_upstream()
        self._subscribers.append(subscriber)

    def close(self) -> None:
        """Release subscriber references.

        Call once the pipeline has completed to allow downstream links to be
        garbage-collected.  Safe to call multiple times.
        """
        self._subscribers.clear()

    async def publish(self, datum: Any) -> None:
        if not self._subscribers:
            return
        if len(self._subscribers) == 1:
            await self._subscribers[0].push(datum)
        else:
            async with TaskGroup() as tg:
                for subscriber in self._subscribers:
                    tg.create_task(subscriber.push(datum))
