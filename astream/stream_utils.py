from __future__ import annotations

import asyncio
import functools
from abc import abstractmethod
from datetime import timedelta
from itertools import chain
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    cast,
    Coroutine,
    Iterable,
    ParamSpec,
    Protocol,
    runtime_checkable,
    TypeVar,
    overload,
)

from astream import NoValueSentinel, SentinelType
from astream import Stream
from astream import ensure_async_iterator, ensure_coro_fn

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


@overload
def stream(
    fn: Iterable[_T] | AsyncIterable[_T],
) -> Stream[_T]:
    ...


@overload
def stream(
    fn: Callable[_P, AsyncIterable[_T]] | Callable[_P, Iterable[_T]],
) -> Callable[_P, Stream[_T]]:
    ...


def stream(
    fn: Callable[_P, AsyncIterable[_T]]
    | Callable[_P, Iterable[_T]]
    | AsyncIterable[_T]
    | Iterable[_T],
) -> Callable[_P, Stream[_T]] | Stream[_T]:
    """A decorator that turns a generator or async generator function into a stream."""

    if isinstance(fn, AsyncIterable) or isinstance(fn, Iterable):
        return Stream(fn)

    _fn = fn

    @functools.wraps(_fn)
    def _wrapped(*args: _P.args, **kwargs: _P.kwargs) -> Stream[_T]:
        return Stream(_fn(*args, **kwargs))

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
    return Stream(iterable).aflatmap(fn)  # type: ignore


arange = stream(range)


@stream
async def arange_delayed(
    start: int,
    stop: int | None = None,
    step: int = 1,
    delay: timedelta = timedelta(seconds=0.2),
) -> AsyncIterator[int]:
    if stop is None:
        stop = start
        start = 0
    for i in range(start, stop, step):
        yield i
        await asyncio.sleep(delay.total_seconds())


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


async def areduce(
    fn: Callable[[_T, _U], Coroutine[Any, Any, _T]] | Callable[[_T, _U], _T],
    iterable: AsyncIterable[_U],
    initial: _T | SentinelType = NoValueSentinel,
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
        crt = await _fn_async(crt, item)  # type: ignore
    return crt


NoData = object()


def dotget(path: str) -> Callable[[Any], Iterable[Any]]:
    def _adotget(p: str, *objs: Any) -> Any:
        if not p:
            yield from objs

        part, parts = p.split(".", maxsplit=1) if "." in p else (p, "")

        matches = []
        for obj in objs:

            match obj:
                case dict() if part in obj:
                    matches.append(_adotget(parts, obj[part]))
                case list() if part.isdigit() and int(part) < len(obj):
                    matches.append(_adotget(parts, obj[int(part)]))
                case dict() if part == "*":
                    for v in obj.values():
                        matches.append(_adotget(parts, v))
                case list() if part == "*":
                    for v in obj:
                        matches.append(_adotget(parts, v))
                case _ if (result := getattr(obj, part, NoData)) is not NoData:
                    matches.append(_adotget(parts, result))
                case _:
                    pass

        yield from chain.from_iterable(matches)

    def _call(*objs: Any) -> Any:
        return _adotget(path, *objs)

    return _call


__all__ = (
    "stream",
    "aenumerate",
    "agetitem",
    "agetattr",
    "afilter",
    "amap",
    "aflatmap",
    "arange",
    "arange_delayed",
    "amerge",
    "ascan",
    "areduce",
    "dotget",
)
