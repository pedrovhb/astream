from __future__ import annotations

import asyncio
import functools
from asyncio import Future, Queue, Task
from enum import Enum
from types import NotImplementedType
from typing import (
    Annotated,
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Generic,
    Iterable,
    Literal,
    NoReturn,
    ParamSpec,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)

P = ParamSpec("P")
T = TypeVar("T")


def run_sync(f: Callable[P, Coroutine[Any, Any, T]]) -> Callable[P, T]:
    """Given a function, return a new function that runs the original one with asyncio.

    This can be used to transparently wrap asynchronous functions. It can be used for example to
    use an asynchronous function as an entry point to a `Typer` CLI.

    Args:
        f: The function to run synchronously.

    Returns:
        A new function that runs the original one with `asyncio.run`.
    """

    @functools.wraps(f)
    def decorated(*args: P.args, **kwargs: P.kwargs) -> T:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(f(*args, **kwargs))

    return decorated


async def arange(start: int, stop: int | None = None, step: int = 1) -> AsyncIterator[int]:
    """An asynchronous version of `range`."""
    if stop is None:
        stop = start
        start = 0

    for i in range(start, stop, step):
        yield i
