from typing import Any


class EndOfStream:
    """Sentinel signalling end of data in a stream. Singleton."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "END_OF_STREAM"


class Skip:
    """Sentinel returned by a transformer to drop the current item from the stream."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self):
        return "SKIP"


END_OF_STREAM = EndOfStream()
SKIP = Skip()


class ErrorEnvelope:
    """
    Wraps an item that caused an exception in a processor.

    Travels down error edges in the graph.  Error-handling links receive
    this, inspect it, and either:
      - emit the original datum (for retry back-edges to re-deliver it)
      - emit Skip (item is dead-lettered or suppressed)
      - re-raise (propagate failure)

    Attributes
    ----------
    datum      : the original item that caused the error
    exc        : the exception raised
    link_name  : name of the link that failed
    retry_count: how many times this item has already been retried
    handled    : set to True by Publisher.publish_error() before routing
    """
    __slots__ = ('datum', 'exc', 'link_name', 'retry_count', 'handled')

    def __init__(
        self,
        datum: Any,
        exc: Exception,
        link_name: str = '',
        retry_count: int = 0,
    ) -> None:
        self.datum = datum
        self.exc = exc
        self.link_name = link_name
        self.retry_count = retry_count
        self.handled: bool = False

    def __repr__(self) -> str:
        return (
            f'ErrorEnvelope(link={self.link_name!r}, exc={self.exc!r}, '
            f'retry={self.retry_count}, handled={self.handled})'
        )
