from __future__ import annotations

import asyncio
from typing import TypeVar
from asyncio.queues import (
    Queue as AsyncioQueue,
    PriorityQueue as AsyncioPriorityQueue,
    LifoQueue as AsyncioLifoQueue,
)

from asyncutils.protocols import CloseableQueueLike

T = TypeVar("T")


class QueueClosed(Exception):
    ...


class QueueExhausted(Exception):
    ...


class CloseableQueue(AsyncioQueue[T], CloseableQueueLike[T, T]):
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

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)
        self._closed_future = asyncio.Future[None]()
        self._exhausted_future = asyncio.Future[None]()

    async def put(self, item: T) -> None:
        """Put an item into the queue.

        Raises:
            QueueClosed: If the queue is closed.

        Returns:
            None
        """
        if self.is_closed:
            raise QueueClosed()

        return await super().put(item)

    def put_nowait(self, item: T) -> None:
        """Put an item into the queue without blocking.

        Raises:
            QueueClosed: If the queue is closed.

        Returns:
            None
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

        get_task = asyncio.create_task(super().get())
        closed_task = asyncio.create_task(self.wait_closed())
        done, pending = await asyncio.wait(
            (get_task, closed_task), return_when=asyncio.FIRST_COMPLETED
        )
        try:
            if get_task in done:
                item = get_task.result()
                if self.empty() and self.is_closed:
                    self._set_exhausted()
                return item
            else:
                raise QueueExhausted()
        finally:
            for task in pending:
                task.cancel()

    def get_nowait(self) -> T:
        """Remove and return an item from the queue without blocking.

        Raises:
            QueueExhausted: If the queue is closed and empty.
            QueueEmpty: If the queue is not closed and empty.

        Returns:
            The item from the queue.
        """
        if self.is_exhausted:
            raise QueueExhausted()
        item = super().get_nowait()
        if self.empty() and self.is_closed:
            self._set_exhausted()

        return item

    @staticmethod
    def _set_if_not_done(future: asyncio.Future[None]) -> None:
        if not future.done():
            future.set_result(None)

    def close(self) -> None:
        self._set_if_not_done(self._closed_future)
        if self.empty():
            self._set_if_not_done(self._exhausted_future)

    def _set_exhausted(self) -> None:
        self._set_if_not_done(self._exhausted_future)

    @property
    def is_closed(self) -> bool:  # type: ignore
        return self._closed_future.done()

    async def wait_closed(self) -> None:
        await self._closed_future

    @property
    def is_exhausted(self) -> bool:  # type: ignore
        return self._exhausted_future.done()

    async def wait_exhausted(self) -> None:
        await self._exhausted_future
        await self.join()


class CloseablePriorityQueue(AsyncioPriorityQueue[T], CloseableQueueLike[T, T]):
    """A closeable version of PriorityQueue."""


class CloseableLifoQueue(AsyncioLifoQueue[T], CloseableQueueLike[T, T]):
    """A closeable version of LifoQueue."""
