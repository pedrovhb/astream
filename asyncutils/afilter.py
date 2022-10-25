from __future__ import annotations

import asyncio
from asyncio import Queue
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    ParamSpec,
    TypeVar,
    cast,
    TypeGuard,
)


T = TypeVar("T")
U = TypeVar("U")

_A = TypeVar("_A")
_B = TypeVar("_B")
_C = TypeVar("_C")

_TrueT = TypeVar("_TrueT")
_FalseT = TypeVar("_FalseT")

P = ParamSpec("P")


class _SentinelType:
    """An object representing a sentinel command."""


async def iter_to_aiter(iterable: Iterable[T]) -> AsyncIterator[T]:
    """Convert an iterable to an async iterable (running the iterable in a background thread)."""

    done = _SentinelType()
    q = Queue[T | _SentinelType]()
    loop = asyncio.get_running_loop()

    def _inner() -> None:
        for it in iterable:
            loop.call_soon_threadsafe(q.put_nowait, it)
        loop.call_soon_threadsafe(q.put_nowait, done)

    asyncio.create_task(asyncio.to_thread(_inner))

    while not q.empty():
        item = await q.get()
        if item is done:
            return
        yield cast(T, item)


async def filter_async_iterable(
    func: Callable[[T], Coroutine[Any, Any, bool]] | Callable[[T], bool],
    iterable: AsyncIterable[T],
    exc_handler: Callable[[tuple[T, BaseException]], Any]
    | Callable[[tuple[T, BaseException]], Coroutine[Any, Any, Any]]
    | None = None,
) -> AsyncIterator[T]:
    """Filter an async iterable using a predicate.

    Args:
        iterable: The async iterable to filter.
        func: The predicate to filter the async iterable with.
        exc_handler: A callable or async callable to execute when an Exception occurs

    Yields:
        The items in the async iterable for which the predicate returned True.
    """
    if asyncio.iscoroutinefunction(func):
        _fn = cast(Callable[[T], Coroutine[Any, Any, bool]], func)
        async for item in iterable:

            try:

                if await _fn(item):
                    yield item

            except Exception as exc:
                if asyncio.iscoroutinefunction(exc_handler):
                    await exc_handler((item, exc))
                elif exc_handler is not None:
                    exc_handler((item, exc))
                else:
                    raise
    else:
        _fn_sync = cast(Callable[[T], bool], func)
        async for item in iterable:
            try:

                if _fn_sync(item):
                    yield item

            except Exception as exc:
                if asyncio.iscoroutinefunction(exc_handler):
                    await exc_handler((item, exc))
                elif exc_handler is not None:
                    exc_handler((item, exc))
                else:
                    raise


afilter = filter_async_iterable
i2a = iter_to_aiter
