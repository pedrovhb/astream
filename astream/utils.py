from __future__ import annotations

import asyncio
import functools
import time
from asyncio import Future
from typing import *

from .sentinel import _RaiseExceptionT, Sentinel

_T = TypeVar("_T")
_U = TypeVar("_U")
_R = TypeVar("_R")
_P = ParamSpec("_P")
_CoroT: TypeAlias = Coroutine[Any, Any, _T]
_ItemAndFut: TypeAlias = Future[tuple[_T, "_ItemAndFut[_T]"]]


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
    result: list[_T] = []

    def run_iterator() -> None:
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
def ensure_coroutine_function(
    fn: Callable[_P, _CoroT[_T]], to_thread: bool = ...
) -> Callable[_P, _CoroT[_T]]:
    ...


@overload
def ensure_coroutine_function(
    fn: Callable[_P, _T], to_thread: bool = ...
) -> Callable[_P, _CoroT[_T]]:
    ...


def ensure_coroutine_function(
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


@overload
def run_stream(
    stream: AsyncIterable[_T],
    if_empty: _RaiseExceptionT = Sentinel.RaiseException,
) -> _T:
    ...


@overload
def run_stream(
    stream: AsyncIterable[_T],
    if_empty: _U = ...,
) -> _T | _U:
    ...


def run_stream(
    stream: AsyncIterable[_T],
    if_empty: _U | _RaiseExceptionT = Sentinel.RaiseException,
) -> _T | _U:
    """Run an async iterable to completion.

    Args:
        stream: The async iterable to run.
        if_empty: The value to return if the stream is empty. Defaults to raising an exception.

    Returns:
        The last item yielded by the async iterable.

    Raises:
        ValueError: If the async iterable yields no items.

    Examples:
        >>> from astream.stream_utils import arange
        >>> print(run_stream(arange(5) / (lambda x: x * 5)))
    """

    async def _runner(_stream: AsyncIterable[_T]) -> _T | _U:
        item: _T | _U | _RaiseExceptionT = if_empty
        async for item in _stream:
            pass

        if item is Sentinel.RaiseException:
            raise ValueError("stream yielded no items")

        return item

    return asyncio.run(_runner(stream))


__all__ = (
    "run_sync",
    "iter_to_aiter",
    "ensure_coroutine_function",
    "ensure_async_iterator",
    "create_future",
)
