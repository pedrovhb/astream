from __future__ import annotations

import asyncio
from asyncio import CancelledError
from asyncio.queues import LifoQueue as AsyncioLifoQueue
from asyncio.queues import PriorityQueue as AsyncioPriorityQueue
from asyncio.queues import Queue as AsyncioQueue
from collections.abc import AsyncIterable
from typing import Any, AsyncIterator, Coroutine, Iterable, TypeAlias, TypeVar

from astream import Stream, ensure_async_iterator

T = TypeVar("T")
Coro: TypeAlias = Coroutine[Any, Any, T]


class _CloseableQueueIterator(AsyncIterable[T]):
    """An async iterator that yields items from a queue."""

    def __init__(self, queue: CloseableQueue[T]) -> None:
        self._queue = queue

    def __aiter__(self) -> _CloseableQueueIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            item = await self._queue.get()
            self._queue.task_done()
            return item
        except CancelledError:
            raise
        except QueueExhausted:
            self._queue.close()
            raise StopAsyncIteration
        except Exception as e:
            self._queue.close()
            raise e


class QueueClosed(Exception):
    ...


class QueueExhausted(Exception):
    ...


class CloseableQueue(AsyncioQueue[T], AsyncIterable[T]):
    """A closeable version of the asyncio.Queue class.

    This class is a closeable version of the asyncio.Queue class.

    It adds the `close` method, which closes the queue. Once the queue is closed, attempts to put
    items into it will raise `QueueClosed`. Items can still be removed until the closed queue is
    empty, at which point it is considered exhausted. Attempts to get items from an exhausted
    queue will raise `QueueExhausted`.

    The `wait_closed` and `wait_exhausted` methods can be used to wait for the queue to be closed
    or exhausted, respectively.

    Calling `put` or `put_nowait` on a closed queue will raise `QueueClosed`, and calling `get`
    or `get_nowait` on an exhausted queue will raise `QueueExhausted`.
    """

    _putters: list[asyncio.Future[None]]
    _getters: list[asyncio.Future[None]]
    _finished: asyncio.Event

    _queue: Iterable[T]

    _close_getters: list[asyncio.Future[T]]
    _iter_queues: list[CloseableQueue[T]]

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._closed = asyncio.Event()
        self._exhausted = asyncio.Event()

        self._close_getters = []
        self._iter_queues = []

    async def put(self, item: T) -> None:
        """Put an item into the queue.

        Raises:
            QueueClosed: If the queue is closed.
        """
        if self.is_closed:
            raise QueueClosed()
        await super().put(item)
        for iter_q in self._iter_queues:
            iter_q.put_nowait(item)

    def put_nowait(self, item: T) -> None:
        """Put an item into the queue without blocking.

        Raises:
            QueueFull: If the queue is full.
            QueueClosed: If the queue is closed.
        """
        if self.is_closed:
            raise QueueClosed()
        super().put_nowait(item)
        for iter_q in self._iter_queues:
            iter_q.put_nowait(item)

    async def get(self) -> T:
        """Remove and return an item from the queue.

        Raises:
            QueueExhausted: If the queue is closed and empty.

        Returns:
            The item from the queue.
        """

        if self.is_exhausted:
            raise QueueExhausted()
        try:
            return await super().get()
        except CancelledError:
            if self.is_exhausted:
                raise QueueExhausted()
            raise

    def get_nowait(self) -> T:
        """Remove and return an item from the queue without blocking.

        Raises:
            QueueEmpty: If the queue is empty.
            QueueExhausted: If the queue is closed and empty.

        Returns:
            The item from the queue.
        """
        if self.is_exhausted:
            raise QueueExhausted()

        return super().get_nowait()

    def task_done(self) -> None:
        super(CloseableQueue, self).task_done()
        if self.is_closed and self._finished.is_set():
            self._set_exhausted()

    def close(self) -> None:
        self._closed.set()

        for putter in self._putters:
            putter.set_exception(QueueClosed())

        for iter_q in self._iter_queues:
            iter_q.close()

        if self._finished.is_set():
            self._set_exhausted()

    def _set_exhausted(self) -> None:
        self._exhausted.set()
        for getter in self._getters:
            getter.cancel()

    @property
    def is_closed(self) -> bool:
        return self._closed.is_set()

    async def wait_closed(self) -> None:
        await self._closed.wait()

    @property
    def is_exhausted(self) -> bool:
        return self._exhausted.is_set()

    async def wait_exhausted(self) -> None:
        await self._exhausted.wait()

    def __aiter__(self) -> Stream[T]:
        return Stream(self._aiter())

    async def _aiter(self) -> AsyncIterator[T]:
        while True:
            try:
                item = await self.get()
                self.task_done()
                yield item
            except QueueExhausted:
                break

    async def feed_from(
        self,
        source: Iterable[T] | AsyncIterable[T],
        close_when_done: bool = False,
    ) -> None:
        """Feed items from an async iterator into the queue.

        This method will feed items from the given async iterator into the queue. It will
        automatically close the queue when the source is exhausted.

        Args:
            source: The async iterator to feed items from.
            close_when_done: If True, the queue will be closed when the source is exhausted.
        """
        async for item in ensure_async_iterator(source):
            await self.put(item)

        if close_when_done:
            self.close()

    def __lshift__(self, other: Iterable[T] | AsyncIterable[T]) -> None:
        """Feed items from an iterator or async iterator into the queue.

        Args:
            other: The async iterator to feed items from.
        """
        asyncio.create_task(self.feed_from(other))

    def __rrshift__(self, other: Iterable[T] | AsyncIterable[T]) -> None:
        """Feed items from an iterator or async iterator into the queue.

        Args:
            other: The async iterator to feed items from.
        """
        asyncio.create_task(self.feed_from(other))

    # def __repr__(self) -> str:
    #     return (
    #         f"{self.__class__.__name__}("
    #         f"max={self.maxsize}, crt={self.qsize()}, getters={self._getters}, "
    #         f"putters={self._putters}, closed={self.is_closed}, exhausted={self.is_exhausted}"
    #         f")"
    #     )


class CloseablePriorityQueue(AsyncioPriorityQueue[T], CloseableQueue[T]):
    """A closeable version of PriorityQueue."""


class CloseableLifoQueue(AsyncioLifoQueue[T], CloseableQueue[T]):
    """A closeable version of LifoQueue."""


__all__ = (
    "CloseableQueue",
    "CloseablePriorityQueue",
    "CloseableLifoQueue",
    "QueueClosed",
    "QueueExhausted",
)
