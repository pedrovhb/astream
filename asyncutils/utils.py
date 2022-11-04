from __future__ import annotations

import asyncio
import functools
import inspect
from abc import abstractmethod
from asyncio import Future
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    cast,
    Coroutine,
    Hashable,
    Iterable,
    Mapping,
    overload,
    ParamSpec,
    runtime_checkable,
    Sequence,
    SupportsIndex,
    TypeVar,
)

from typing_extensions import Protocol

from asyncutils.stream import Stream

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


@overload
async def apluck(iterable: AsyncIterable[Sequence[_T]], key: SupportsIndex) -> AsyncIterator[_T]:
    ...


@overload
async def apluck(
    iterable: AsyncIterable[Mapping[Hashable, _VT]], key: Hashable
) -> AsyncIterator[_VT]:
    ...


async def apluck(
    iterable: AsyncIterable[SupportsGetItem[Hashable, _VT]] | AsyncIterable[Sequence[_T]],
    key: SupportsIndex | Hashable,
) -> AsyncIterator[_T] | AsyncIterator[_VT]:
    """An asynchronous version of `pluck`."""

    async for item in iterable:
        if isinstance(item, Sequence) and isinstance(key, SupportsIndex):
            yield item[key]
        else:
            yield item[key]


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


# todo - ascan


def arange(start: int, stop: int | None = None, step: int = 1) -> AsyncIterable[int]:
    """An asynchronous version of `range`.

    Args:
        start: The start of the range.
        stop: The end of the range.
        step: The step of the range.

    Yields:
        The next item in the range.

    Examples:
        >>> async def demo_arange():
        ...     async for i in arange(5):
        ...         print(i)
        >>> asyncio.run(demo_arange())
        0
        1
        2
        3
        4
    """
    if stop is None:
        stop = start
        start = 0
    return Stream(range(start, stop, step))


def run_sync(f: Callable[_P, Coroutine[Any, Any, _T]]) -> Callable[_P, _T]:
    """Given a function, return a new function that runs the original one with asyncio.

    This can be used to transparently wrap asynchronous functions. It can be used for example to
    use an asynchronous function as an entry point to a `Typer` CLI.

    Args:
        f: The function to run synchronously.

    Returns:
        A new function that runs the original one with `asyncio.run`.
    """

    @functools.wraps(f)
    def decorated(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return decorated


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


if __name__ == "__main__":

    async def main() -> None:
        # s = cast(Stream[Iterable[int]], (aenumerate(Stream(range(100, 110)) / str)))
        # todo - figure out how to make iterable detection work
        # s = aenumerate(arange(100, 110))
        # async for i in +s:
        #     print(i)

        # async for i in Stream(range(10)):
        #     print(i)
        def t():
            from itertools import cycle

            return stream(cycle)(range(10))

        # todo - make import hook context manager to allow import external library functions
        #  as stream-decorated functions

        async for i in t():
            print(i)
            await asyncio.sleep(0.1)

    asyncio.run(main())
