from __future__ import annotations

import asyncio
import functools
from asyncio import Future
from typing import *

from astream.closeable_queue import CloseableQueue

_P = ParamSpec("_P")
_T = TypeVar("_T")


class SentinelType:
    pass


_NoValueSentinelT = NewType("_NoValueSentinelT", SentinelType)
NoValueSentinel = _NoValueSentinelT(SentinelType())  # noqa


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


def _iter_to_aiter_threaded(iterable: Iterable[_T]) -> AsyncIterator[_T]:
    """Convert an iterable to an async iterable (running the iterable in a background thread)."""

    q = CloseableQueue[_T]()
    loop = asyncio.get_running_loop()

    def _inner() -> None:
        for it in iterable:
            loop.call_soon_threadsafe(q.put_nowait, it)
        loop.call_soon_threadsafe(q.close)

    asyncio.create_task(asyncio.to_thread(_inner))
    return aiter(q)


def iter_to_aiter(iterable: Iterable[_T], to_thread: bool) -> AsyncIterator[_T]:
    """Convert an iterable to an async iterable.

    Args:
        iterable: The iterable to convert.
        to_thread: Whether to run the iterable in a background thread.

    Returns:
        An async iterable.
    """

    if to_thread:
        return _iter_to_aiter_threaded(iterable)

    @functools.wraps(iterable.__iter__)
    async def _inner() -> AsyncIterator[_T]:
        for it in iterable:
            yield it
            print("put", it, "in iter_to_aiter")
            await asyncio.sleep(0)

    return _inner()


def ensure_coro_fn(
    fn: Callable[_P, _T] | Callable[_P, Coroutine[Any, Any, _T]],
    to_thread: bool = False,
) -> Callable[_P, Coroutine[Any, Any, _T]]:
    """Given a sync or async function, return an async function.

    Args:
        fn: The function to ensure is async.
        to_thread: Whether to run the function in a thread, if it is sync.

    Returns:
        An async function that runs the original function.
    """

    if asyncio.iscoroutinefunction(fn):
        return fn

    _sync_fn = cast(Callable[_P, _T], fn)
    if to_thread:

        @functools.wraps(fn)
        async def _async_fn(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            return await asyncio.to_thread(_sync_fn, *args, **kwargs)

    else:

        @functools.wraps(fn)
        async def _async_fn(*args: _P.args, **kwargs: _P.kwargs) -> _T:
            return _sync_fn(*args, **kwargs)

    return _async_fn


def ensure_async_iterator(
    iterable: Iterable[_T] | AsyncIterable[_T],
    to_thread: bool = True,
) -> AsyncIterator[_T]:
    """Given an iterable or async iterable, return an async iterable.

    Args:
        iterable: The iterable to ensure is async.
        to_thread: Whether to run the iterable in a thread, if it is sync.

    Returns:
        An async iterable that runs the original iterable.
    """

    if isinstance(iterable, AsyncIterable):
        return aiter(iterable)

    return aiter(iter_to_aiter(iterable, to_thread=to_thread))


def create_future() -> Future[_T]:
    return asyncio.get_running_loop().create_future()


__all__ = (
    "run_sync",
    "iter_to_aiter",
    "ensure_coro_fn",
    "ensure_async_iterator",
    "create_future",
    "NoValueSentinel",
    "SentinelType",
)
