from __future__ import annotations

import asyncio
from asyncio import CancelledError
from asyncio.queues import (
    LifoQueue as AsyncioLifoQueue,
    PriorityQueue as AsyncioPriorityQueue,
    Queue as AsyncioQueue,
)
from collections.abc import AsyncIterable
from typing import Any, Coroutine, TypeAlias, TypeVar

from astream.protocols.queue import QueueProtocol

T = TypeVar("T")
Coro: TypeAlias = Coroutine[Any, Any, T]


class QueueClosed(Exception):
    ...


class QueueExhausted(Exception):
    ...


class CloseableQueue(AsyncioQueue[T], AsyncIterable[T], QueueProtocol[T, T]):
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

    _close_getters: list[asyncio.Future[T]]

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._closed = asyncio.Event()
        self._exhausted = asyncio.Event()

        self._close_getters = []

    async def put(self, item: T) -> None:
        """Put an item into the queue.

        Raises:
            QueueClosed: If the queue is closed.
        """
        if self.is_closed:
            raise QueueClosed()
        return await super().put(item)

    def put_nowait(self, item: T) -> None:
        """Put an item into the queue without blocking.

        Raises:
            QueueFull: If the queue is full.
            QueueClosed: If the queue is closed.
        """
        if self.is_closed:
            raise QueueClosed()
        return super().put_nowait(item)

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

    def __aiter__(self) -> CloseableQueue[T]:
        return self

    async def __anext__(self) -> T:
        try:
            item = await self.get()
            self.task_done()
            return item
        except QueueExhausted:
            raise StopAsyncIteration

    # def __repr__(self) -> str:
    #     return (
    #         f"{self.__class__.__name__}("
    #         f"max={self.maxsize}, crt={self.qsize()}, getters={self._getters}, "
    #         f"putters={self._putters}, closed={self.is_closed}, exhausted={self.is_exhausted}"
    #         f")"
    #     )


class CloseablePriorityQueue(AsyncioPriorityQueue[T], CloseableQueue[T], QueueProtocol[T,T]):
    """A closeable version of PriorityQueue."""


class CloseableLifoQueue(AsyncioLifoQueue[T], CloseableQueue[T], QueueProtocol[T,T]):
    """A closeable version of LifoQueue."""


__all__ = (
    "CloseableQueue",
    "CloseablePriorityQueue",
    "CloseableLifoQueue",
    "QueueClosed",
    "QueueExhausted",
)
