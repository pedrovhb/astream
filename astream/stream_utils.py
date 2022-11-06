from __future__ import annotations

import asyncio
import functools
from abc import abstractmethod
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    cast,
    Coroutine,
    Iterable,
    ParamSpec,
    runtime_checkable,
    TypeVar,
)

from typing_extensions import Protocol

from astream.stream import Stream
from astream.utils import _SentinelT, ensure_async_iterator, ensure_coro_fn, NoValueSentinel

_T = TypeVar("_T")
_U = TypeVar("_U")

_P = ParamSpec("_P")

_KT = TypeVar("_KT", contravariant=True)
_VT = TypeVar("_VT", covariant=True)


@runtime_checkable
class SupportsGetItem(Protocol[_KT, _VT]):
    """A protocol for objects that support `__getitem__`."""

    @abstractmethod
    def __getitem__(self, key: _KT) -> _VT:
        ...


def stream(
    fn: Callable[_P, AsyncIterable[_T]] | Callable[_P, Iterable[_T]],
) -> Callable[_P, Stream[_T]]:
    """A decorator that turns a generator or async generator function into a stream."""

    @functools.wraps(fn)
    def _wrapped(*args: _P.args, **kwargs: _P.kwargs) -> Stream[_T]:
        return Stream(fn(*args, **kwargs))

    return _wrapped


@stream
async def aenumerate(iterable: AsyncIterable[_T], start: int = 0) -> AsyncIterator[tuple[int, _T]]:
    """An asynchronous version of `enumerate`."""
    async for item in iterable:
        yield start, item
        start += 1


@stream
async def agetitem(
    iterable: AsyncIterable[SupportsGetItem[_KT, _VT]],
    key: _KT,
) -> AsyncIterator[_VT]:
    """An asynchronous version of `getitem`."""
    async for item in iterable:
        yield item[key]


@stream
async def agetattr(
    iterable: AsyncIterable[object],
    name: str,
) -> AsyncIterator[Any]:
    """An asynchronous version of `getattr`."""
    async for item in iterable:
        yield getattr(item, name)


def afilter(
    fn: Callable[[_T], Coroutine[Any, Any, bool]] | Callable[[_T], bool],
    iterable: AsyncIterable[_T],
) -> Stream[_T]:
    """An asynchronous version of `filter`."""
    return Stream(iterable).afilter(fn)


def amap(
    fn: Callable[[_T], Coroutine[Any, Any, _U]] | Callable[[_T], _U],
    iterable: AsyncIterable[_T],
) -> Stream[_U]:
    """An asynchronous version of `map`."""
    return Stream(iterable).amap(fn)


def aflatmap(
    fn: Callable[[_T], Coroutine[Any, Any, Iterable[_U]]]
    | Callable[[_T], AsyncIterable[_U]]
    | Callable[[_T], Iterable[_U]],
    iterable: AsyncIterable[_T],
) -> Stream[_U]:
    """An asynchronous version of `flatmap`."""
    return Stream(iterable).aflatmap(fn)


# @stream
# def arange(start: int, stop: int | None = None, step: int = 1) -> Iterator[int]:
#     """An asynchronous version of `range`.
#
#     Args:
#         start: The start of the range.
#         stop: The end of the range.
#         step: The step of the range.
#
#     Yields:
#         The next item in the range.
#
#     Examples:
#         >>> async def demo_arange():
#         ...     async for i in arange(5):
#         ...         print(i)
#         >>> asyncio.run(demo_arange())
#         0
#         1
#         2
#         3
#         4
#     """
#     import time
#
#     if stop is None:
#         stop = start
#         start = 0
#     for i in range(start, stop, step):
#         time.sleep(1)
#         yield i
#         print(f"yielded {i}")
#     print("done")


arange = stream(range)


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
async def ascan(
    fn: Callable[[_T, _U], Coroutine[Any, Any, _T]] | Callable[[_T, _U], _T],
    iterable: AsyncIterable[_U],
    initial: _T | _SentinelT = NoValueSentinel,
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
        crt = await _fn_async(crt, item)
        yield crt


async def areduce(
    fn: Callable[[_T, _U], Coroutine[Any, Any, _T]] | Callable[[_T, _U], _T],
    iterable: AsyncIterable[_U],
    initial: _T | _SentinelT = NoValueSentinel,
) -> _T:
    """An asynchronous version of `reduce`.

    Args:
        fn: The function to reduce with.
        iterable: The iterable to reduce.
        initial: The initial value to reduce with.

    Returns:
        The reduced value.

    Examples:
        >>> async def demo_areduce():
        ...     print(await areduce(lambda a, b: a + b, arange(5)))
        >>> asyncio.run(demo_areduce())
        10

        >>> async def demo_areduce():
        ...     print(await areduce(lambda a, b: a + b, arange(5), 5))
        >>> asyncio.run(demo_areduce())
        15
    """
    _fn_async = ensure_coro_fn(fn)
    _it_async = ensure_async_iterator(iterable)

    if initial is NoValueSentinel:
        initial = await anext(_it_async)  # type: ignore
    crt = cast(_T, initial)

    async for item in _it_async:
        crt = await _fn_async(crt, item)
    return crt
