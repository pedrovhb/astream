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

    def __init__(self, *async_iterables: AsyncIterable[T]) -> None:
        self._fut_to_aiter = dict[Future[T], AsyncIterator[T]]()
        for async_iter in async_iterables:
            self.add_async_iter(async_iter)
        self._merged_aiter_instance = aiter(self._merged_aiter())

    def add_async_iter(self, async_iter: AsyncIterable[T]) -> None:
        async_iterator = aiter(async_iter)
        queue_getter = anext(async_iterator)
        fut = asyncio.ensure_future(queue_getter)
        self._fut_to_aiter[fut] = async_iterator

    async def _merged_aiter(self) -> AsyncIterator[T]:
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
