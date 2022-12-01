from __future__ import annotations

import asyncio
import functools
import time
from asyncio import Future
from queue import Queue
from typing import *
from typing import NewType

_T = TypeVar("_T")
_R = TypeVar("_R")
_P = ParamSpec("_P")
_CoroT: TypeAlias = Coroutine[Any, Any, _T]
_ItemAndFut: TypeAlias = Future[tuple[_T, "_ItemAndFut[_T]"]]


class SentinelType:
    pass


class NoValueT:
    """A singleton sentinel value to indicate no value."""

    instance: NoValueT

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls, *args, **kwargs)
        return cls.instance


NoValue = NoValueT()


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


async def iter_to_aiter(iterable: Iterable[_T], /, target_dt: float = 0.0005) -> AsyncIterator[_T]:
    """Convert an iterable to an async iterator, running the iterable in a thread.

    Args:
        iterable: The iterable to convert.
        target_dt: The target maximum time to possibly block the event loop for. Defaults to 0.0005.
            Note that this is not a hard limit, and the actual time spent blocking the event loop
            may be longer than this. See also sys.getswitchinterval().

    Yields:
        Items from the iterable.

    Examples:
        >>> async def demo_iter_to_thread():
        ...     async for item in iter_to_aiter(range(5)):
        ...         print(item)
        >>> asyncio.run(demo_iter_to_thread())
        0
        1
        2
        3
        4
    """
    iterator = iter(iterable)
    loop = asyncio.get_running_loop()
    result = []

    def run_iterator():
        # We assign variables to the function scope to avoid the overhead of
        # looking them up in the closure scope during the hot loop. We also
        # reuse the same list to avoid the overhead of allocating a new list
        # every time, and have it in shared memory to avoid the overhead of
        # copying the list to the main thread.
        _result_append = result.append
        _iterator_next = iterator.__next__
        _time_monotonic = time.monotonic
        _target_t = _time_monotonic() + target_dt

        try:
            while _time_monotonic() < _target_t:
                _result_append(_iterator_next())
        except StopIteration:
            raise StopAsyncIteration

    while True:
        try:
            # Run the iterator in a thread until we've reached the target time.
            await loop.run_in_executor(None, run_iterator)

            # Yield the results.
            for item in result:
                yield item

            # Clear the results.
            result.clear()
        except StopAsyncIteration:
            # Yield the remaining items in the result buffer.
            for item in result:
                yield item
            return


@overload
def ensure_coro_fn(fn: Callable[_P, _CoroT[_T]], to_thread: bool = ...) -> Callable[_P, _CoroT[_T]]:
    ...


@overload
def ensure_coro_fn(fn: Callable[_P, _T], to_thread: bool = ...) -> Callable[_P, _CoroT[_T]]:
    ...


def ensure_coro_fn(
    fn: Callable[_P, _T] | Callable[_P, _CoroT[_T]], to_thread: bool = False
) -> Callable[_P, _CoroT[_T]]:
    """Given a sync or async function, return an async function.

    Args:
        fn: The function to ensure is async.
        to_thread: Whether to run the function in a thread, if it is sync.

    Returns:
        An async function that runs the original function.
    """

    if asyncio.iscoroutinefunction(fn):
        return fn

    _fn_sync = cast(Callable[_P, _T], fn)
    if to_thread:

        @functools.wraps(_fn_sync)
        async def _async_fn(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            return await asyncio.to_thread(_fn_sync, *args, **kwargs)

    else:

        @functools.wraps(_fn_sync)
        async def _async_fn(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            return _fn_sync(*args, **kwargs)

    return _async_fn


@overload
def ensure_async_iterator(iterable: Iterable[_T]) -> AsyncIterator[_T]:
    ...


@overload
def ensure_async_iterator(iterable: AsyncIterable[_T]) -> AsyncIterator[_T]:
    ...


def ensure_async_iterator(iterable: Iterable[_T] | AsyncIterable[_T]) -> AsyncIterator[_T]:
    """Given an iterable or async iterable, return an async iterable.

    Args:
        iterable: The iterable to ensure is async.
        to_thread: Whether to run the iterable in a thread, if it is sync.

    Returns:
        An async iterable that runs the original iterable.
    """

    if isinstance(iterable, AsyncIterable):
        return aiter(iterable)

    return aiter(iter_to_aiter(iterable))


def create_future() -> Future[_T]:
    return asyncio.get_running_loop().create_future()


__all__ = (
    "run_sync",
    "iter_to_aiter",
    "ensure_coro_fn",
    "ensure_async_iterator",
    "create_future",
    "SentinelType",
    "NoValueT",
    "NoValue",
)
