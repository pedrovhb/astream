from __future__ import annotations

import asyncio
import sys
from functools import partial
from typing import AsyncIterable, TypeVar, AsyncIterator

from .sentinel import Sentinel, _NoValueT
from .stream_utils import arange
from .stream import sink, Stream

_T = TypeVar("_T")


@sink
async def to_stdout(
    async_iterator: AsyncIterator[bytes],
    line_separator: bytes = b"\n",
    redirect_stdout: bool = False,
) -> None:
    """Write lines to stdout.

    Examples:
        >>> async def demo_to_stdout() -> None:
        ...     await Stream([b"hello", b"world"]) / to_stdout()
        >>> asyncio.run(demo_to_stdout())
        hello
        world
    """
    out = sys.stdout
    if redirect_stdout:
        sys.stdout = sys.stderr

    # Write to stdout. On the first iteration, we don't print the separator.
    # On subsequent iterations, print a newline, and then the contents.
    try:
        out.buffer.write(await anext(async_iterator))
        out.flush()
    except StopAsyncIteration:
        pass
    else:
        async for item in async_iterator:
            out.buffer.write(line_separator)
            out.buffer.write(item)
            out.flush()


@sink
async def last(async_iterable: AsyncIterable[_T]) -> _T:
    """Get the last item from the stream.

    Examples:
        >>> async def demo_last() -> None:
        ...     print(await last(range(3)))
        >>> asyncio.run(demo_last())
        2
    """
    prev: _NoValueT | _T = Sentinel.NoValue
    async for item in async_iterable:
        print(item)
        prev = item

    if prev is Sentinel.NoValue:
        raise ValueError("async_iterable is empty")
    else:
        return prev


@sink
async def nth(async_iterable: AsyncIterable[_T], n: int) -> _T:
    """Get the nth item from the stream.

    Examples:
        >>> async def demo_nth() -> None:
        ...     print(await nth(arange(3), 1))
        >>> asyncio.run(demo_nth())
        1
    """
    async_iterator = aiter(async_iterable)
    for _ in range(n):
        await anext(async_iterator)
    return await anext(async_iterator)


first = partial(nth, n=0)
