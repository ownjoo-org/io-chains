from __future__ import annotations

from asyncio import TaskGroup
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any

from io_chains._internal.sentinel import EndOfStream
from io_chains._internal.subscriber import Subscriber

if TYPE_CHECKING:
    from io_chains._internal.sentinel import ErrorEnvelope


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
        self._on_metrics: Callable | None = None
        self._subscribers: list[Subscriber] = []
        self._error_subscribers: list[Subscriber] = []
        self._items_out: int = 0
        self.on_metrics = on_metrics
        self.subscribers = subscribers

    @property
    def on_metrics(self) -> Callable | None:
        return self._on_metrics

    @on_metrics.setter
    def on_metrics(self, value: Callable | None) -> None:
        if value is not None and not callable(value):
            raise TypeError(f"on_metrics must be callable or None, got {type(value)}")
        self._on_metrics = value

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    @property
    def items_out(self) -> int:
        return self._items_out

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

    @property
    def error_subscribers(self) -> list['Subscriber']:
        return self._error_subscribers

    @error_subscribers.setter
    def error_subscribers(self, subscribers: 'Subscriber | Iterable[Subscriber] | None') -> None:
        if not subscribers:
            return
        if isinstance(subscribers, Subscriber):
            self._error_subscribers.append(subscribers)
        elif isinstance(subscribers, Iterable):
            for s in subscribers:
                if not isinstance(s, Subscriber):
                    raise TypeError(f'each error_subscriber must be a Subscriber, got {type(s)}')
                self._error_subscribers.append(s)
        else:
            raise TypeError('error_subscribers must be a Subscriber or Iterable[Subscriber]')

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
        self._error_subscribers.clear()

    async def publish_error(self, envelope: 'ErrorEnvelope') -> bool:
        """Publish an ErrorEnvelope to error_subscribers.

        Returns True if at least one error subscriber received it (handled),
        False if no error subscribers are registered (caller should re-raise).
        """
        if not self._error_subscribers:
            return False
        envelope.handled = True
        if len(self._error_subscribers) == 1:
            await self._error_subscribers[0].push(envelope)
        else:
            async with TaskGroup() as tg:
                for sub in self._error_subscribers:
                    tg.create_task(sub.push(envelope))
        return True

    async def publish(self, datum: Any) -> None:
        if not isinstance(datum, EndOfStream):
            self._items_out += 1
        if not self._subscribers:
            return
        if len(self._subscribers) == 1:
            await self._subscribers[0].push(datum)
        else:
            async with TaskGroup() as tg:
                for subscriber in self._subscribers:
                    tg.create_task(subscriber.push(datum))
