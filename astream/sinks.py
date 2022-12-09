from __future__ import annotations

import asyncio
import sys
from collections import deque
from functools import partial
from typing import AsyncIterator, TypeVar

from astream.stream import sink, Stream
from astream.stream_utils import arange

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
        # If the stream is empty (i.e. raises StopAsyncIteration on first iteration), just fly away
        return
    else:
        async for item in async_iterator:
            out.buffer.write(line_separator)
            out.buffer.write(item)
            out.flush()


@sink
async def nth(async_iterator: AsyncIterator[_T], n: int) -> _T:
    """Get the nth item from the stream (0-indexed).

    A negative integer returns the nth from last item.

    Examples:
        >>> async def demo_nth() -> None:
        ...     print(await nth(arange(3), 1))
        >>> asyncio.run(demo_nth())
        1
    """
    async_iterator = aiter(async_iterator)

    if n >= 0:
        for _ in range(n):
            await anext(async_iterator)
        return await anext(async_iterator)
    else:
        items: deque[_T] = deque(maxlen=-n)
        async for item in async_iterator:
            items.append(item)
        return items[0]


first = partial(nth, n=0)
last = partial(nth, n=-1)

if __name__ == "__main__":

    async def main() -> None:
        assert await (arange(100) / nth(-4)) == 96
        assert await (arange(100) / nth(4)) == 4

    print(list(range(100))[-1])

    asyncio.run(main())


__all__ = ("first", "last", "nth", "to_stdout")
