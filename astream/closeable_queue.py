from __future__ import annotations

from asyncio import Future, LifoQueue, PriorityQueue, Queue, QueueEmpty, Task
import asyncio
from typing import *

from astream.event_like import Fuse

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
    """A Queue that can be closed, and that can be iterated over asynchronously.

    This class is a subclass of `asyncio.Queue` that adds the ability to close
    the queue, preventing any further items from being added. It also adds the
    ability to iterate over the queue asynchronously, using the `__aiter__`
    method.

    The queue can be closed using the `close` method. Once closed, the queue
    will raise a `QueueClosed` exception if an attempt is made to add an item.
    The `is_closed` property can be used to check if the queue is closed.

    The queue can be iterated over asynchronously using the `__aiter__` method.
    This method returns an asynchronous iterator that can be used in an
    `async for` loop. The iterator will raise a `QueueExhausted` exception if
    the queue is exhausted before the loop is finished. The `is_exhausted`
    property can be used to check if the queue is exhausted.

    The queue can be waited on to be closed, exhausted, or finished using the
    `wait_closed`, `wait_exhausted`, and `wait_finished` coroutines. The
    `wait_closed` coroutine will wait until the queue is closed. The
    `wait_exhausted` coroutine will wait until the queue is exhausted. The
    `wait_finished` coroutine will wait until the queue is exhausted and all
    async iterators have been stopped.
    """

    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize=maxsize)
        loop = asyncio.get_event_loop()

        self._cq_closed = loop.create_future()
        self._cq_exhausted = loop.create_future()
        self._cq_finished = loop.create_future()

        self._aiter_done_futs: set[Future[None]] = set()

    async def get(self) -> _T:
        if self._cq_exhausted.done():
            raise QueueExhausted()

        getter_task = asyncio.create_task(super().get())
        done, pending = await asyncio.wait(
            (getter_task, self._cq_exhausted),
            return_when=asyncio.FIRST_COMPLETED,
        )

        if getter_task in done:
            return getter_task.result()
        else:
            raise QueueExhausted()

    def get_nowait(self) -> _T:
        if self._cq_exhausted.done():
            raise QueueExhausted()
        return super().get_nowait()

    async def put(self, item: _T) -> None:
        if self.is_closed:
            raise QueueClosed()

        putter_task = asyncio.create_task(super().put(item))
        done, pending = await asyncio.wait(
            (putter_task, self._cq_closed),
            return_when=asyncio.FIRST_COMPLETED,
        )

        if putter_task in done:
            return
        else:
            raise QueueClosed()

    def put_nowait(self, item: _T) -> None:
        if self.is_closed:
            raise QueueClosed()
        super().put_nowait(item)

    def task_done(self) -> None:
        super().task_done()
        if self.empty() and self.is_closed and not self._aiter_done_futs:
            self._cq_exhausted.set_result(None)

    def close(self) -> None:
        """Close the queue, preventing any further items from being added."""
        self._cq_closed.set_result(None)

        if self.empty():
            self._cq_exhausted.set_result(None)
        for fut in self._aiter_done_futs:
            fut.set_exception(QueueExhausted())
        if not self._aiter_done_futs:
            self._cq_finished.set_result(None)

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
                    (queue_get_task, self._cq_exhausted),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if queue_get_task in done:
                    yield queue_get_task.result()
                    self.task_done()

                if self._cq_exhausted.done():
                    return
                elif self.empty() and self._cq_closed.done() and not self._cq_exhausted.done():
                    self._cq_exhausted.set_result(None)
                    return

        finally:

            if queue_get_task is not None and not queue_get_task.done():
                queue_get_task.cancel()

            self._aiter_done_futs.remove(done_fut)

            # If exhausted and no more iterators, set the queue as finished.
            # This is meant to be called by the last iterator alive, to signal
            # that the queue is finished. If there are no iterators, the queue
            # is set as finished in _finalize.
            if self._cq_exhausted.done() and not self._aiter_done_futs:
                self._cq_finished.done()

    async def wait_closed(self) -> None:
        """Wait for the queue to be closed."""
        await self._cq_closed

    async def wait_exhausted(self) -> None:
        """Wait for the queue to be exhausted."""
        await self._cq_exhausted

    async def wait_finished(self) -> None:
        """Wait for the queue to be finished."""
        await self._cq_finished

    @property
    def is_closed(self) -> bool:
        """Return True if the queue is closed."""
        return self._cq_closed.done()

    @property
    def is_exhausted(self) -> bool:
        """Return True if the queue is exhausted."""
        return self._cq_exhausted.done()

    @property
    def is_finished(self) -> bool:
        """Return True if the queue is exhausted and all async gens have been stopped."""
        return self._cq_finished.done()

    def __aiter__(self) -> AsyncIterator[_T]:
        if self._cq_exhausted.done():
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
    """Feed an async queue from an async iterable.

    This coroutine will feed the queue with items from the iterable until the
    iterable is exhausted. If `close_when_done` is True, the queue will be
    closed when the iterable is exhausted.

    Args:
        queue: The queue to feed.
        iterable: The iterable to feed the queue from.
        close_when_done: If True and the queue is CloseableQueue or one of its subclasses, the
            queue will be closed when the iterable is exhausted.

    Raises:
        QueueClosed: If the queue is closed before the iterable is exhausted.
        ValueError: If the queue is not a CloseableQueue or one of its subclasses and
            `close_when_done` is True.
    """
    if close_when_done and not isinstance(queue, CloseableQueue):
        raise ValueError("close_when_done requires a CloseableQueue or one of its subclasses")

    async for item in iterable:
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


__all__ = (
    "CloseableLifoQueue",
    "CloseablePriorityQueue",
    "CloseableQueue",
    "empty_gen",
    "feed_queue",
    "QueueClosed",
    "QueueExhausted",
)
