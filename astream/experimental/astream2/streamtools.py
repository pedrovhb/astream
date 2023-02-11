from __future__ import annotations

from collections import deque
from typing import AsyncIterator

from asynkets import ensure_coroutine_function

from astream.stream import transformer, _U, _T, sink, AnyFunction


@transformer
async def aenumerate(
    stream: AsyncIterator[_U], start: int = 0
) -> AsyncIterator[tuple[int, _U]]:
    """Given a stream, return a stream of tuples of the index and item."""
    i = start
    async for item in stream:
        yield i, item
        i += 1


@transformer
async def pairwise(stream: AsyncIterator[_T]) -> AsyncIterator[tuple[_T, _T]]:
    """Given a stream, return a stream of tuples of the previous and next
    items.
    """
    ait = aiter(stream)
    try:
        prev = await anext(ait)
        current = await anext(ait)
        yield prev, current
    except StopAsyncIteration:
        return
    prev = current
    async for current in ait:
        yield prev, current
        prev = current


@transformer
async def nwise(stream: AsyncIterator[_T], n: int) -> AsyncIterator[tuple[_T, ...]]:
    """Given a stream, return a stream of tuples of the n previous items."""
    container: deque[_T] = deque(maxlen=n)
    async for item in stream:
        container.append(item)
        if len(container) == n:
            yield tuple(container)


@sink
async def count_elements(stream: AsyncIterator[_T], start_count: int = 0) -> int:
    """Given a stream, return the number of items in the stream."""
    count = start_count
    async for _ in stream:
        count += 1
    return count


@transformer
async def batched(
    stream: AsyncIterator[_T], batch_size: int
) -> AsyncIterator[tuple[_T, ...]]:
    """Given a stream, return a stream of batches of items."""
    _batch: list[_T] = []
    async for item in stream:
        _batch.append(item)
        if len(_batch) == batch_size:
            yield tuple(_batch)
            _batch.clear()
    if _batch:
        yield _batch


@transformer
async def take(stream: AsyncIterator[_T], n: int) -> AsyncIterator[_T]:
    """Given a stream, return a stream of the first n items."""
    if n <= 0:
        return
    i = 0
    async for item in stream:
        yield item
        i += 1
        if i == n:
            return


@transformer
async def drop(stream: AsyncIterator[_T], n: int) -> AsyncIterator[_T]:
    """Given a stream, return a stream of the items after the first n items."""
    if n <= 0:
        return
    i = 0
    async for item in stream:
        i += 1
        if i > n:
            yield item


@transformer
async def take_while(
    stream: AsyncIterator[_T],
    predicate: AnyFunction[[_T], bool],
) -> AsyncIterator[_T]:
    """Given a stream, return a stream of the items while the predicate is true."""
    predicate_fn = ensure_coroutine_function(predicate)
    async for item in stream:
        if await predicate_fn(item):
            yield item
        else:
            return


@transformer
async def drop_while(
    stream: AsyncIterator[_T],
    predicate: AnyFunction[[_T], bool],
) -> AsyncIterator[_T]:
    """Given a stream, return a stream of the items after the predicate is false."""
    predicate_fn = ensure_coroutine_function(predicate)
    async for item in stream:
        if await predicate_fn(item):
            continue
        else:
            yield item
            break
    async for item in stream:
        yield item


__all__ = (
    "aenumerate",
    "batched",
    "count_elements",
    "drop",
    "drop_while",
    "nwise",
    "pairwise",
    "take",
    "take_while",
)
