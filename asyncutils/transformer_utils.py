from __future__ import annotations

import asyncio
import functools
import inspect
from itertools import chain
from typing import ParamSpec, TypeVar, Any, cast, Awaitable, overload
from collections.abc import AsyncIterator, Callable, AsyncIterable, Coroutine, Iterable

from asyncutils.afilter import _SentinelType
from asyncutils.transformer import Stream

_P = ParamSpec("_P")
_T = TypeVar("_T")
_U = TypeVar("_U")

NoInitialValue = _SentinelType()


def stream(fn: Callable[_P, AsyncIterable[_T]]) -> Callable[_P, Stream[_T]]:
    @functools.wraps(fn)
    def _wrapped(*args: _P.args, **kwargs: _P.kwargs) -> Stream[_T]:
        return Stream(fn(*args, **kwargs))

    return _wrapped


@stream
async def apartition(it: AsyncIterable[_T], size: int) -> AsyncIterator[tuple[_T, ...]]:
    """Yield tuples of items from an async iterator, with a maximum size of `size`.

    Args:
        it: The async iterator to partition.
        size: The maximum size of the partitions.

    Yields:
        Tuples of items from the async iterator with a maximum size of `size`.

    Examples:
        >>> async def a():
        ...     for i in range(10):
        ...         yield i
        >>> async def demo_apartition():
        ...     async for partitioned in apartition(a(), 3):
        ...         print(partitioned)
        >>> asyncio.run(demo_apartition())
        (0, 1, 2)
        (3, 4, 5)
        (6, 7, 8)
        (9,)
    """
    batch = []
    async for item in it:
        batch.append(item)
        if len(batch) == size:
            yield tuple(batch)
            batch = []
    if batch:
        yield tuple(batch)


@stream
async def apairwise(it: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T]]:
    """Yield (prev, crt) pairs of items from an async iterator.

    Args:
        it: The async iterator to partition.

    Yields:
        Pairs of items from the async iterator.

    Examples:
        >>> async def a():
        ...     for i in range(5):
        ...         yield i
        >>> async def demo_apairwise():
        ...     async for pair in apairwise(a()):
        ...         print(pair)
        >>> asyncio.run(demo_apairwise())
        (0, 1)
        (1, 2)
        (2, 3)
        (3, 4)
    """
    ait = aiter(it)
    prev = await anext(ait)
    async for item in ait:
        yield prev, item
        prev = item


@stream
async def amerge(*async_iters: AsyncIterable[_T]) -> AsyncIterator[_T]:
    """Merge multiple async iterators into one, yielding items as they are received.

    Args:
        async_iters: The async iterators to merge.

    Yields:
        Items from the async iterators, as they are received.

    Examples:
        >>> async def a():
        ...     for i in range(3):
        ...         await asyncio.sleep(0.025)
        ...         yield i
        >>> async def b():
        ...     for i in range(100, 106):
        ...         await asyncio.sleep(0.01)
        ...         yield i
        >>> async def demo_amerge():
        ...     async for item in amerge(a(), b()):
        ...         print(item)
        >>> asyncio.run(demo_amerge())
        100
        101
        0
        102
        103
        1
        104
        105
        2
    """
    futs: dict[asyncio.Future[_T], AsyncIterator[_T]] = {}
    for it in async_iters:
        async_it = aiter(it)
        fut = asyncio.ensure_future(anext(async_it))
        futs[fut] = async_it

    while futs:
        done, _ = await asyncio.wait(futs, return_when=asyncio.FIRST_COMPLETED)
        for done_fut in done:
            try:
                yield done_fut.result()
            except StopAsyncIteration:
                pass
            else:
                fut = asyncio.ensure_future(anext(futs[done_fut]))
                futs[fut] = futs[done_fut]
            finally:
                del futs[done_fut]


@stream
async def azip(*async_iters: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, ...]]:
    """Zip multiple async iterators into one, yielding tuples of items as they are received.

    Args:
        async_iters: The async iterators to zip.

    Yields:
        Tuples of items from the async iterators, as they are received.

    Examples:
        >>> async def a():
        ...     for i in range(6):
        ...         await asyncio.sleep(0.25)
        ...         yield i
        >>> async def b():
        ...     for i in range(100, 106):
        ...         await asyncio.sleep(0.1)
        ...         yield i
        >>> async def demo_azip():
        ...     async for item_a, item_b in azip(a(), b()):
        ...         print(item_a, item_b)
        >>> asyncio.run(demo_azip())
        0 100
        1 101
        2 102
        3 103
        4 104
        5 105
    """
    async_iterators = tuple(aiter(it) for it in async_iters)
    while True:
        try:
            yield tuple(await asyncio.gather(*(anext(it) for it in async_iterators)))
        except StopAsyncIteration:
            break


@stream
async def ascan(
    it: AsyncIterable[_T],
    func: Callable[[_T, _T], _T] | Callable[[_T, _T], Coroutine[Any, Any, _T]],
    initial: _T | _SentinelType = NoInitialValue,
) -> AsyncIterator[_T]:
    """Yield the accumulated result of applying the function to the items of the async iterator.

    Args:
        it: The async iterator to scan.
        func: The function to apply to the items of the async iterator.
        initial: The initial value to use for the accumulated result, if any.

    Yields:
        The accumulated result of applying the function to the items of the async iterator.

    Examples:
        >>> async def a():
        ...     for i in range(5):
        ...         yield i
        >>> async def demo_ascan():
        ...     async for scan_item in ascan(a(), (lambda x, y: x + y), 0):
        ...         print(scan_item)
        >>> asyncio.run(demo_ascan())
        0
        1
        3
        6
        10
    """
    ait = aiter(it)
    if initial is NoInitialValue:
        value = await anext(ait)
    else:
        assert not isinstance(initial, _SentinelType)
        value = initial

    if inspect.iscoroutinefunction(func):
        _async_func = func
        async for item in ait:
            value = await _async_func(value, item)
            yield value
    else:
        _sync_func = cast(Callable[[_T, _T], _T], func)
        async for item in ait:
            value = _sync_func(value, item)
            yield value


@stream
async def aenumerate(it: AsyncIterable[_T], start: int = 0) -> AsyncIterator[tuple[int, _T]]:
    """Yield (index, item) pairs from an async iterator.

    Args:
        it: The async iterator to enumerate.
        start: The index to start from.

    Yields:
        (index, item) pairs from the async iterator.

    Examples:
        >>> async def a():
        ...     for ch in "abcde":
        ...         yield ch
        >>> async def demo_aenumerate():
        ...     async for enumerated_item in aenumerate(a()):
        ...         print(enumerated_item)
        >>> asyncio.run(demo_aenumerate())
        (0, 'a')
        (1, 'b')
        (2, 'c')
        (3, 'd')
        (4, 'e')
    """
    index = start
    async for item in it:
        yield index, item
        index += 1


@stream
async def afilter(
    it: AsyncIterable[_T],
    func: Callable[[_T], bool] | Callable[[_T], Coroutine[Any, Any, bool]],
) -> AsyncIterator[_T]:
    """Yield items from an async iterator that satisfy a predicate.

    Args:
        it: The async iterator to filter.
        func: The predicate to apply to the items of the async iterator.

    Yields:
        Items from the async iterator that satisfy the predicate.

    Examples:
        >>> async def a():
        ...     for i in range(5):
        ...         yield i
        >>> async def demo_afilter():
        ...     async for filtered_item in afilter(a(), (lambda x: x % 2 == 0)):
        ...         print(filtered_item)
        >>> asyncio.run(demo_afilter())
        0
        2
        4
    """
    if inspect.iscoroutinefunction(func):
        _async_func = func
        async for item in it:
            if await _async_func(item):
                yield item
    else:
        _sync_func = cast(Callable[[_T], bool], func)
        async for item in it:
            if _sync_func(item):
                yield item


@stream
def amap(
    it: AsyncIterable[_T],
    func: Callable[[_T], _U] | Callable[[_T], Coroutine[Any, Any, _U]],
    to_thread: bool = False,
) -> Stream[_U]:
    """Yield the result of applying a function to the items of an async iterator.

    Args:
        it: The async iterator to map.
        func: The function to apply to each item of the async iterator.
        to_thread: If True and the function is sync, run it in a thread.

    Yields:
        The result of applying the function to the items of the async iterator.

    Examples:
        >>> async def a():
        ...     for i in range(5):
        ...         yield i
        >>> async def demo_amap():
        ...     async for mapped_item in amap(a(), (lambda x: x * 2)):
        ...         print(mapped_item)
        >>> asyncio.run(demo_amap())
        0
        2
        4
        6
        8
    """
    if inspect.isasyncgenfunction(func):
        raise TypeError("func must be a sync or async function")

    if to_thread and not inspect.iscoroutinefunction(func):
        _sync_func = cast(Callable[[_T], _U], func)

        @functools.wraps(func)
        async def _func(item: _T) -> _U:
            return await asyncio.to_thread(_sync_func, item)

        func = _func
    return Stream.from_map(it, func)


def aflatmap(
    it: AsyncIterable[_T],
    func: Callable[[_T], Iterable[_U]]
    | Callable[[_T], Coroutine[Any, Any, Iterable[_U]]]
    | Callable[[_T], AsyncIterable[_U]],
) -> Stream[_U]:
    """Yield the items of the async iterators returned by a function applied to the items of an async iterator.

    Args:
        it: The async iterator to flatmap.
        func: A sync or async function which accepts an item from the async iterator and returns
            one of the following:
            - An iterable of items to yield.
            - An async iterable of items to yield.

    Yields:
        Each item from the unpacked return value of the applied function.

    Examples:
        >>> async def a():
        ...     for i in range(10, 13):
        ...         yield range(8, i)
        >>> async def demo_aflatmap():
        ...     async for flatmapped_item in aflatmap(a(), list):
        ...         print(flatmapped_item)
        >>> asyncio.run(demo_aflatmap())
        8
        9
        8
        9
        10
        8
        9
        10
        11
    """
    return Stream.from_flatmap(it, func)


@overload
def agather(it: AsyncIterable[_T], func: Callable[[list[_T]], _U]) -> Awaitable[_U]:
    ...


@overload
def agather(
    it: AsyncIterable[_T], func: Callable[[list[_T]], Coroutine[Any, Any, _U]]
) -> Awaitable[_U]:
    ...


def agather(it: AsyncIterable[_T], func: Any) -> Awaitable[_U]:
    """Gather the items of an async iterator into a list and apply a function to it.

    Args:
        it: The async iterator to gather.
        func: The function to apply to the gathered items.

    Returns:
        The result of applying the function to the gathered items.

    Examples:
        >>> async def a():
        ...     for i in range(5):
        ...         yield i
        >>> async def demo_agather():
        ...     result = await agather(a(), sum)
        ...     print(result)
        >>> asyncio.run(demo_agather())
        10

        >>> async def demo_agather_2():
        ...     result = await agather(a(), list)
        ...     print(result)
        >>> asyncio.run(demo_agather_2())
        [0, 1, 2, 3, 4]
    """
    return Stream(it).agather(func)
