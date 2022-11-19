from __future__ import annotations

import asyncio
import functools
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


def _iter_to_aiter_threaded(iterable: Iterable[_T]) -> AsyncIterator[_T]:
    """Convert an iterable to an async iterable (running the iterable in a background thread)."""

    stop_sentinel = SentinelType()

    def _iter_to_queue() -> None:
        for it in iterable:
            queue.put(it)
        queue.put(stop_sentinel)

    queue: Queue[_T | SentinelType] = Queue(maxsize=100_000)

    loop = asyncio.get_running_loop()
    loop.run_in_executor(None, _iter_to_queue)

    async def _inner() -> AsyncIterator[_T]:
        while True:
            while not queue.empty():
                it = queue.get_nowait()
                if it is stop_sentinel:
                    return
                assert not isinstance(it, SentinelType)
                yield it
                await asyncio.sleep(0)
            await asyncio.sleep(0)

    return _inner()


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
    async def _inner() -> AsyncIterator[T]:
        for it in iterable:
            yield it
            await asyncio.sleep(0)

    return _inner()


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
def ensure_async_iterator(iterable: Iterable[_T], to_thread: bool = ...) -> AsyncIterator[_T]:
    ...


@overload
def ensure_async_iterator(iterable: AsyncIterable[_T], to_thread: bool = ...) -> AsyncIterator[_T]:
    ...


def ensure_async_iterator(
    iterable: Iterable[_T] | AsyncIterable[_T],
    to_thread: bool = False,
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
    "SentinelType",
    "NoValueT",
    "NoValue",
)
