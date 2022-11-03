from __future__ import annotations

import asyncio
from asyncio import Future
from typing import Generic, AsyncIterable, AsyncIterator, TypeVar

T = TypeVar("T")


class amerge(Generic[T], AsyncIterable[T]):  # noqa
    """An async iterable that merges multiple async iterables.

    Items from each async iterable are yielded through this single async iterable as they come in.
    It is possible to add more async iterables to the merged async iterable after it has been
    created by calling its `add_async_iter` method.

    Args:
        async_iterables: The async iterables to merge.
    """

    __slots__ = (
        "_fut_to_aiter",
        "_merged_aiter_instance",
    )

    def __init__(self, *async_iterables: AsyncIterable[T], close_when_done: bool = False) -> None:
        self._fut_to_aiter = dict[Future[T], AsyncIterator[T]]()
        self.close_when_done = close_when_done
        self._closed = asyncio.Future[None]()
        self._has_items = asyncio.Event()

        for async_iter in async_iterables:
            self.add_async_iter(async_iter)
        self._merged_aiter_instance = self._merged_aiter()

    def close(self) -> None:
        self._closed.set_result(None)

    def add_async_iter(self, async_iter: AsyncIterable[T]) -> None:
        async_iterator = aiter(async_iter)
        queue_getter = anext(async_iterator)
        fut = asyncio.ensure_future(queue_getter)
        self._fut_to_aiter[fut] = async_iterator
        self._has_items.set()

    async def _merged_aiter(self) -> AsyncIterator[T]:

        crt_task = asyncio.current_task()
        if crt_task is None:
            raise RuntimeError()
        self._closed.add_done_callback(crt_task.cancel)

        while True:

            done_, pending_ = await asyncio.wait(
                (self._closed, has_items_fut := asyncio.ensure_future(self._has_items.wait())),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if has_items_fut not in done_:  # self._close future has resolved; return
                return

            while self._fut_to_aiter:
                done, pending = await asyncio.wait(
                    self._fut_to_aiter.keys(),
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for done_future in done:

                    if exc := done_future.exception():
                        if isinstance(exc, StopAsyncIteration):
                            self._fut_to_aiter.pop(done_future)
                            continue
                        else:
                            raise exc

                    future_aiter = self._fut_to_aiter.pop(done_future)
                    new_future = asyncio.ensure_future(anext(future_aiter))
                    self._fut_to_aiter[new_future] = future_aiter
                    yield done_future.result()

            if self.close_when_done and not self._has_items.is_set():
                return

            self._has_items.clear()

    def __aiter__(self) -> AsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        return await anext(self._merged_aiter_instance)


async def gather_async_iterables(*async_iterables: AsyncIterable[T]) -> list[T]:
    """Gather the results from multiple async iterators.

    This is useful for when you want to gather the results from multiple async iterables
    without having to iterate over them.

    Args:
        async_iterables: The async iterables to gather the results from.

    Returns:
        A list of the results from the async iterables in the order in which they were completed.
    """
    return [item async for item in amerge(*async_iterables)]
