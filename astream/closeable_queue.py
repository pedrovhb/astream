from __future__ import annotations

import asyncio
import random
from asyncio import (
    CancelledError,
    Event,
    Future,
    LifoQueue,
    PriorityQueue,
    Queue,
    QueueEmpty,
    shield,
    Task,
)
from functools import cached_property
from typing import *

import math

from .event_like import Fuse

_T = TypeVar("_T")


async def empty_gen() -> AsyncGenerator[_T, None]:
    # noinspection PyUnreachableCode
    if False:
        yield  # type: ignore


class QueueClosed(Exception):
    """Raised when trying to put items into a closed queue."""


class QueueExhausted(QueueEmpty):
    """Raised when trying to get items from a closed and empty queue.

    An exhausted queue is a queue that is closed and empty, and thus will never
    yield any more items.
    """

class CloseableQueue(Queue[_T]):
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize=maxsize)

        self._cq_closed = Fuse()
        self._cq_exhausted = Fuse()
        self._cq_finished = Fuse()

        self._finalize_task: Task[None] | None = None
        self._aiter_done_futs: set[Future[None]] = set()

    def _put_closed(self, item: _T) -> NoReturn:
        raise QueueClosed()

    def get_exhausted(self) -> NoReturn:
        raise QueueExhausted()

    def close(self) -> None:
        """Close the queue, preventing any further items from being added."""
        self._cq_closed.set()

        # self.put = self.put_nowait = self._put_closed  todo readd

        if self._finalize_task is None:
            self._finalize_task = asyncio.create_task(self._finalize())

        # Cancelling the finalize task should not be possible, as it is
        # responsible for setting the queue as finished. To prevent it
        # from being cancelled by the user while still allowing close()
        # to return an awaitable, we return a future which is set by the
        # finalize task.

        # fut = asyncio.get_event_loop().create_future()
        # self._finalize_task.add_done_callback(
        #     lambda _: fut.set_result(None) if not fut.done() else None
        # )

    async def _finalize(self) -> None:
        """Finalize the queue, preventing any further items from being retrieved."""

        # Wait for the queue to be empty, i.e. all items have been consumed
        await self.join()
        self._cq_exhausted.set()

        # Prevent getting from the exhausted queue
        # self.get = self.get_nowait = self.get_exhausted  todo readd

        # Set done on all iterators, or directly set _finished if there are none
        if self._aiter_done_futs:
            for fut in self._aiter_done_futs:
                fut.set_result(None)
        else:
            self._cq_finished.set()

    async def _async_iterator(self) -> AsyncIterator[_T]:
        queue_get_task: Task[_T] | None = None
        done_fut = asyncio.get_event_loop().create_future()
        self._aiter_done_futs.add(done_fut)
        try:

            while True:
                # Wait for either an item to be available, or for the done_fut
                # Future to be set (indicating that the queue has been closed and
                # exhausted).
                queue_get_task = asyncio.create_task(self.get())
                done, pending = await asyncio.wait(
                    (done_fut, queue_get_task), return_when=asyncio.FIRST_COMPLETED
                )

                if queue_get_task in done:
                    yield queue_get_task.result()
                    self.task_done()

                if done_fut in done:
                    return

        finally:

            if queue_get_task is not None and not queue_get_task.done():
                queue_get_task.cancel()

            if not done_fut.done():
                done_fut.set_result(None)

            self._aiter_done_futs.remove(done_fut)

            # If exhausted and no more iterators, set the queue as finished.
            # This is meant to be called by the last iterator alive, to signal
            # that the queue is finished. If there are no iterators, the queue
            # is set as finished in _finalize.
            if self.is_exhausted and not self._aiter_done_futs:
                self._cq_finished.set()

    async def wait_closed(self) -> None:
        """Wait for the queue to be closed."""
        await self._cq_closed.wait()

    async def wait_exhausted(self) -> None:
        """Wait for the queue to be exhausted."""
        await self._cq_exhausted.wait()

    async def wait_finished(self) -> None:
        """Wait for the queue to be finished."""
        await self._cq_finished.wait()

    @property
    def is_closed(self) -> bool:
        """Return True if the queue is closed."""
        return self._cq_closed.is_set()

    @property
    def is_exhausted(self) -> bool:
        """Return True if the queue is exhausted."""
        return self._cq_exhausted.is_set()

    @property
    def is_finished(self) -> bool:
        """Return True if the queue is exhausted and all async gens have been stopped."""
        return self._cq_finished.is_set()

    def __aiter__(self) -> AsyncIterator[_T]:
        if self._cq_exhausted.is_set():
            raise QueueExhausted()
        return self._async_iterator()


class CloseablePriorityQueue(CloseableQueue[_T], PriorityQueue[_T]):
    pass


class CloseableLifoQueue(CloseableQueue[_T], LifoQueue[_T]):
    pass


async def feed_queue(
    queue: Queue[_T],
    iterable: AsyncIterable[_T],
    close_when_done: bool = False,
) -> None:
    if close_when_done and not isinstance(queue, CloseableQueue):
        raise ValueError("close_when_done requires a CloseableQueue or one of its subclasses")

    async for item in iterable:
        print("putting", item)
        await queue.put(item)

    if close_when_done:
        _queue = cast(CloseableQueue[_T], queue)
        _queue.close()


if __name__ == "__main__":
    import asyncio

    async def main() -> None:
        q = CloseableQueue[int]()
        q2 = CloseableLifoQueue[int]()

        async def arange(n: int) -> AsyncIterable[int]:
            for i in range(n):
                yield i
                await asyncio.sleep(0.1)

        ts = [
            asyncio.create_task(feed_queue(q, arange(10))),
            asyncio.create_task(feed_queue(q2, q)),
            asyncio.create_task(feed_queue(q, q2)),
        ]
        await asyncio.gather(*ts)

        #
        # async def print_queue():
        #     async for item in q:
        #         # break
        #         print(item)
        #
        # async def add_to_queue():
        #     for i in range(10):
        #         await asyncio.sleep(0.1)
        #         await q.put(i)
        #     print("done adding")
        #     q.close()
        #
        # await asyncio.gather(print_queue(), add_to_queue())
        # await q.wait_finished()
        # print("done")
        # q.get_nowait()

    asyncio.run(main(), debug=True)
