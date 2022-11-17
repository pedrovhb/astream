from __future__ import annotations

import asyncio
from abc import abstractmethod
from asyncio import Future
from datetime import timedelta
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
    overload,
    runtime_checkable,
)

from astream import NoValueSentinel, SentinelType, ensure_async_iterator, ensure_coro_fn

_T = TypeVar("_T")
_U = TypeVar("_U")

_CoroT: TypeAlias = Coroutine[Any, Any, _T]

_P = ParamSpec("_P")

_KT = TypeVar("_KT", contravariant=True)
_VT = TypeVar("_VT", covariant=True)

_ItemAndFut: TypeAlias = Future[tuple[_T, "_ItemAndFut[_T]"]]


@runtime_checkable
class SupportsGetItem(Protocol[_KT, _VT]):
    """A protocol for objects that support `__getitem__`."""

    @abstractmethod
    def __getitem__(self, key: _KT) -> _VT:
        ...


async def aenumerate(iterable: AsyncIterable[_T], start: int = 0) -> AsyncIterator[tuple[int, _T]]:
    """An asynchronous version of `enumerate`."""
    async for item in iterable:
        yield start, item
        start += 1


async def agetitem(
    iterable: AsyncIterable[SupportsGetItem[_KT, _VT]],
    key: _KT,
) -> AsyncIterator[_VT]:
    """An asynchronous version of `getitem`."""
    async for item in iterable:
        yield item[key]


async def agetattr(
    iterable: AsyncIterable[object],
    name: str,
) -> AsyncIterator[Any]:
    """An asynchronous version of `getattr`."""
    async for item in iterable:
        yield getattr(item, name)


def afilter(
    fn: Callable[[_T], Coroutine[Any, Any, bool]] | Callable[[_T], bool],
    iterable: AsyncIterable[_T] | Iterable[_T],
) -> AsyncIterator[_T]:
    """An asynchronous version of `filter`."""

    async def _filter() -> AsyncIterator[_T]:
        _fn = ensure_coro_fn(fn)
        _iterable = ensure_async_iterator(iterable)
        async for item in _iterable:
            if await _fn(item):
                yield item

    return _filter()


def atee(
    iterable: AsyncIterable[_T] | Iterable[_T],
    n: int = 2,
) -> tuple[AsyncIterator[_T], ...]:
    """An asynchronous version of `tee`."""

    create_future = asyncio.get_running_loop().create_future

    async def _tee_feeder() -> None:
        async for item in ensure_async_iterator(iterable):
            ks = tuple(futs.keys())
            for fut in ks:
                tee = futs.pop(fut)
                next_fut = create_future()
                futs[next_fut] = tee
                fut.set_result((item, next_fut))
        for fut in futs:
            fut.set_exception(StopAsyncIteration)

    async def _tee(next_fut: _ItemAndFut[_T]) -> AsyncIterator[_T]:
        while True:
            try:
                item, next_fut = await next_fut
            except StopAsyncIteration:
                break
            yield item

    futs = {(f := create_future()): _tee(f) for _ in range(n)}
    asyncio.create_task(_tee_feeder())
    return tuple(futs.values())


@overload
def amap(
    fn: Callable[[_T], _CoroT[_U]], iterable: AsyncIterable[_T] | Iterable[_T]
) -> AsyncIterator[_U]:
    ...


@overload
def amap(fn: Callable[[_T], _U], iterable: AsyncIterable[_T] | Iterable[_T]) -> AsyncIterator[_U]:
    ...


def amap(
    fn: Callable[[_T], Coroutine[Any, Any, _U]] | Callable[[_T], _U],
    iterable: AsyncIterable[_T] | Iterable[_T],
) -> AsyncIterator[_U]:
    """An asynchronous version of `map`."""

    async def _map() -> AsyncIterator[_U]:
        _fn: Callable[[_T], _CoroT[_U]] = ensure_coro_fn(fn)
        _iterable = ensure_async_iterator(iterable)
        async for item in _iterable:
            yield await _fn(item)

    return _map()


@overload
def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: AsyncIterable[Iterable[_T]],
) -> AsyncIterator[_U]:
    ...


@overload
def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: AsyncIterable[AsyncIterable[_T]],
) -> AsyncIterator[_U]:
    ...


@overload
def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: Iterable[Iterable[_T]],
) -> AsyncIterator[_U]:
    ...


@overload
def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: Iterable[AsyncIterable[_T]],
) -> AsyncIterator[_U]:
    ...


def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: AsyncIterable[Iterable[_T]]
    | AsyncIterable[AsyncIterable[_T]]
    | Iterable[Iterable[_T]]
    | Iterable[AsyncIterable[_T]],
) -> AsyncIterator[_U]:
    async def _flatmap() -> AsyncIterator[_U]:
        _fn = ensure_coro_fn(fn)
        _iterable = ensure_async_iterator(iterable)
        async for item in _iterable:
            fn_iter = await _fn(item)
            async for subitem in ensure_async_iterator(fn_iter):
                yield subitem

    return _flatmap()


async def arange_delayed(
    start: int,
    stop: int | None = None,
    step: int = 1,
    delay: timedelta | float = timedelta(seconds=0.2),
) -> AsyncIterator[int]:
    """An asynchronous version of `range` with a delay between each item."""
    _delay = delay.total_seconds() if isinstance(delay, timedelta) else delay
    if stop is None:
        stop = start
        start = 0
    for i in range(start, stop, step):
        yield i
        await asyncio.sleep(_delay)


async def arange(start: int, stop: int | None = None, step: int = 1) -> AsyncIterator[int]:
    """An asynchronous version of `range`.

    Args:
        start: The start of the range.
        stop: The end of the range.
        step: The step of the range.

    Yields:
        The next item in the range.

    Examples:
        >>> async def main():
        ...     async for i in arange(5):
        ...         print(i)
        >>> asyncio.run(main())
        0
        1
        2
        3
        4
    """
    async for i in arange_delayed(start, stop, step, delay=0):
        yield i


def amerge(*async_iters: Iterable[_T] | AsyncIterable[_T]) -> AsyncIterator[_T]:
    """Merge multiple iterables or async iterables into one, yielding items as they are received.

    Args:
        async_iters: The async iterators to merge.

    Yields:
        Items from the async iterators, as they are received.

    Examples:
        >>> async def a():
        ...     for i in range(3):
        ...         await asyncio.sleep(0.07)
        ...         yield i
        >>> async def b():
        ...     for i in range(100, 106):
        ...         await asyncio.sleep(0.03)
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

    async def _inner() -> AsyncIterator[_T]:
        futs: dict[asyncio.Future[_T], AsyncIterator[_T]] = {}
        for it in async_iters:
            async_it = ensure_async_iterator(it)
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

    return _inner()


async def ascan(
    fn: Callable[[_T, _U], Coroutine[Any, Any, _T]] | Callable[[_T, _U], _T],
    iterable: AsyncIterable[_U],
    initial: _T | SentinelType = NoValueSentinel,
) -> AsyncIterator[_T]:
    """An asynchronous version of `scan`.

    Args:
        fn: The function to scan with.
        iterable: The iterable to scan.
        initial: The initial value to scan with.

    Yields:
        The scanned value.

    Examples:
        >>> async def demo_ascan():
        ...     async for it in ascan(lambda a, b: a + b, arange(5)):
        ...         print(it)
        >>> asyncio.run(demo_ascan())
        0
        1
        3
        6
        10
    """
    _fn_async = ensure_coro_fn(fn)
    _it_async = ensure_async_iterator(iterable)

    if initial is NoValueSentinel:
        initial = await anext(_it_async)  # type: ignore
    crt = cast(_T, initial)

    yield crt

    async for item in _it_async:
        crt = await _fn_async(crt, item)  # type: ignore
        yield crt


@overload
def aflatten(iterable: AsyncIterator[Iterable[_T]]) -> AsyncIterator[_T]:
    ...


@overload
def aflatten(iterable: AsyncIterator[AsyncIterable[_T]]) -> AsyncIterator[_T]:
    ...


async def aflatten(
    iterable: AsyncIterator[Iterable[_T]] | AsyncIterator[AsyncIterable[_T]],
) -> AsyncIterator[_T]:
    """Unpacks an async iterator of iterables or async iterables into a flat async iterator."""
    async for item in iterable:
        async for subitem in ensure_async_iterator(item):
            yield subitem


async def aconcatenate(
    *iterables: Iterable[_T] | AsyncIterable[_T],
) -> AsyncIterator[_T]:
    """Concatenates multiple async iterators, yielding all items from the first, then all items
    from the second, etc.
    """
    for iterable in iterables:
        async for item in ensure_async_iterator(iterable):
            yield item


async def arepeat(iterable: Iterable[_T] | AsyncIterable[_T], times: int) -> AsyncIterator[_T]:
    """Repeats an async iterator `times` times."""
    tees = atee(ensure_async_iterator(iterable), times)
    for tee in tees:
        async for item in tee:
            yield item


async def azip(
    *iterables: Iterable[_T] | AsyncIterable[_T],
) -> AsyncIterator[tuple[_T, ...]]:
    """An asynchronous version of `zip`.

    Args:
        *iterables: The iterables to zip.

    Yields:
        The zipped values.

    Examples:
        >>> async def demo_azip():
        ...     async for it in azip(arange(5), arange(5, 10)):
        ...         print(it)
        >>> asyncio.run(demo_azip())
        (0, 5)
        (1, 6)
        (2, 7)
        (3, 8)
        (4, 9)
    """
    async_iterables = tuple(ensure_async_iterator(it) for it in iterables)
    while True:
        try:
            items = await asyncio.gather(*(anext(it) for it in async_iterables))
        except StopAsyncIteration:
            break
        else:
            yield tuple(items)


@overload
def azip_longest(
    *iterables: Iterable[_T] | AsyncIterable[_T],
    fillvalue: None = ...,
) -> AsyncIterator[tuple[_T | None, ...]]:
    ...


@overload
def azip_longest(
    *iterables: Iterable[_T] | AsyncIterable[_T],
    fillvalue: _T = ...,
) -> AsyncIterator[tuple[_T, ...]]:
    ...


async def azip_longest(
    *iterables: Iterable[_T] | AsyncIterable[_T],
    fillvalue: _T | None = None,
) -> AsyncIterator[tuple[_T | None, ...]]:
    """An asynchronous version of `zip_longest`."""
    async_iterables = [ensure_async_iterator(it) for it in iterables]
    while True:
        items = await asyncio.gather(*(anext(it, NoValueSentinel) for it in async_iterables))
        if all(item is NoValueSentinel for item in items):
            break
        yield tuple(item if item is not NoValueSentinel else fillvalue for item in items)


__all__ = (
    "aenumerate",
    "aconcatenate",
    "agetitem",
    "agetattr",
    "afilter",
    "amap",
    "aflatmap",
    "arepeat",
    "arange",
    "atee",
    "arange_delayed",
    "amerge",
    "ascan",
    "aflatten",
)

if __name__ == "__main__":

    async def demo() -> None:
        async for i in aenumerate(arange(10, 20)):
            print(i)

        async for i in aenumerate(arange(10), 5):
            print(i)

        (a, b), c = atee(arange(10, 15), 2), arange(15, 25)
        async for j in a:
            print("a", j)
        async for j in b:
            print("b", j)
        async for j in c:
            print("c", j)

        (a, b), c = atee(arange(10, 15), 2), arange(15, 25)
        async for tup in azip_longest(a, b, c, fillvalue=-1):
            print(tup)

    asyncio.run(demo())
