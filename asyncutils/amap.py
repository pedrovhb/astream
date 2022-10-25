from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar, Coroutine, Callable, AsyncIterator, AsyncIterable, Any, cast


T = TypeVar("T")
U = TypeVar("U")


async def amap(
    func: Callable[[T], Awaitable[U]] | Callable[[T], U],
    async_iterable: AsyncIterable[T],
    exc_handler: Callable[[tuple[T, BaseException]], Any]
    | Callable[[tuple[T, BaseException]], Coroutine[Any, Any, Any]]
    | None = None,
) -> AsyncIterator[U]:
    """Map a function over an async iterable.

    Args:
        func: The function to map.
        async_iterable: The async iterable to map over.
        exc_handler: A callable or async callable to execute when an Exception occurs

    Returns:
        An async iterator over the mapped values.

    Example:
        >>> async def main() -> None:
        ...     from asyncutils.misc import arange
        ...
        ...     async def double(x: int) -> int:
        ...         return x * 2
        ...
        ...     async for i in amap(double, arange(3)):
        ...         print(i)
        ...
        >>> asyncio.run(main())
        0
        2
        4
    """

    if asyncio.iscoroutinefunction(func):
        async for item in async_iterable:
            try:
                yield await func(item)
            except Exception as exc:
                if asyncio.iscoroutinefunction(exc_handler):
                    await exc_handler((item, exc))
                elif exc_handler is not None:
                    exc_handler((item, exc))
                else:
                    raise
    else:
        async for item in async_iterable:
            _sync_func = cast(Callable[[T], U], func)
            try:
                yield _sync_func(item)
            except Exception as exc:
                if asyncio.iscoroutinefunction(exc_handler):
                    await exc_handler((item, exc))
                elif exc_handler is not None:
                    exc_handler((item, exc))
                else:
                    raise
