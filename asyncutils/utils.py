from __future__ import annotations

import asyncio
import functools
import inspect
from abc import abstractmethod
from asyncio import Future
from typing import (
    Iterable,
    AsyncIterable,
    TypeVar,
    Callable,
    Any,
    ParamSpec,
    AsyncIterator,
    runtime_checkable,
    cast,
    Coroutine,
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

    if inspect.isasyncgenfunction(fn):
        _async_fn = cast(Callable[_P, AsyncIterable[_T]], fn)

        @functools.wraps(fn)
        def _wrapped(*args: _P.args, **kwargs: _P.kwargs) -> Stream[_T]:
            return Stream(_async_fn(*args, **kwargs))

    elif inspect.isgeneratorfunction(fn):
        _fn = cast(Callable[_P, Iterable[_T]], fn)

        @functools.wraps(fn)
        def _wrapped(*args: _P.args, **kwargs: _P.kwargs) -> Stream[_T]:
            return Stream(_fn(*args, **kwargs))

    else:
        raise TypeError("fn must be a generator function or async generator function")

    return _wrapped


@stream
async def aenumerate(iterable: AsyncIterable[_T], start: int = 0) -> AsyncIterator[tuple[int, _T]]:
    """An asynchronous version of `enumerate`."""
    async for item in iterable:
        yield start, item
        start += 1


@stream
async def apluck(
    iterable: AsyncIterable[SupportsGetItem[_KT, _VT]],
    key: _KT,
) -> AsyncIterator[_VT]:
    """An asynchronous version of `pluck`."""
    async for item in iterable:
        yield item[key]


async def afilter(
    fn: Callable[[_T], Coroutine[Any, Any, bool]] | Callable[[_T], bool],
    iterable: AsyncIterable[_T],
) -> Stream[_T]:
    """An asynchronous version of `filter`."""
    return Stream(iterable).afilter(fn)


async def amap(
    fn: Callable[[_T], Coroutine[Any, Any, _U]] | Callable[[_T], _U],
    iterable: AsyncIterable[_T],
) -> Stream[_U]:
    """An asynchronous version of `map`."""
    return Stream(iterable).amap(fn)


async def aflatmap(
    fn: Callable[[_T], Coroutine[Any, Any, Iterable[_U]]]
    | Callable[[_T], AsyncIterable[_U]]
    | Callable[[_T], Iterable[_U]],
    iterable: AsyncIterable[_T],
) -> Stream[_U]:
    """An asynchronous version of `flatmap`."""
    return Stream(iterable).aflatmap(fn)


# todo - ascan


@stream
async def arange(start: int, stop: int | None = None, step: int = 1) -> AsyncIterator[int]:
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

    for i in range(start, stop, step):
        yield i


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


def make_queued(
    fn: Callable[_P, Coroutine[Any, Any, _T]],
    n_workers: int,
    queue_size: int = 100,
) -> Callable[_P, Coroutine[Any, Any, _T]]:
    # todo - clean this up
    queue = asyncio.Queue[tuple[tuple[Any, ...], dict[str, Any], Future[_T]]](maxsize=queue_size)

    async def worker() -> None:
        while True:
            args, kwargs, fut = await queue.get()
            try:
                result = await fn(*args, **kwargs)
                fut.set_result(result)
            except Exception as e:
                fut.set_exception(e)
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(n_workers)]

    @functools.wraps(fn)
    async def _wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        fut: Future[_T] = asyncio.get_running_loop().create_future()
        await queue.put((args, kwargs, fut))
        await fut
        return fut.result()

    _wrapped.workers = workers  # type: ignore

    return _wrapped


if __name__ == "__main__":

    async def main() -> None:
        # s = cast(Stream[Iterable[int]], (aenumerate(Stream(range(100, 110)) / str)))
        # todo - figure out how to make iterable detection work
        s = aenumerate(arange(100, 110))
        async for i in +s:
            print(i)

    asyncio.run(main())
