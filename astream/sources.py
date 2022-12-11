from __future__ import annotations

import asyncio
import sys
from functools import partial
from pathlib import Path
from typing import Iterator

from astream.stream import Stream
from astream.stream_utils import bytes_stream_split_separator
from astream.utils import iter_to_aiter


def from_stdin_raw(
    line_separator: bytes = b"\n",
    keep_separator: bool = False,
    strip: bool = True,
) -> Stream[bytes]:
    """Read lines from stdin.

    Examples:
        >>> async def demo_stdin() -> None:
        ...     async for line in from_stdin_raw():
        ...         print(line)
        >>> asyncio.run(demo_stdin())
        hello
        world
    """
    async_iter = Stream(iter_to_aiter(sys.stdin.buffer)).transform(
        bytes_stream_split_separator(separator=line_separator, keep_separator=keep_separator)
    )
    if strip:
        return async_iter.transform(bytes.strip)
    return async_iter


def from_file_raw(
    path: str | Path,
    separator: bytes = b"\n",
    keep_separator: bool = False,
) -> Stream[bytes]:
    def _inner() -> Iterator[bytes]:
        with Path(path).open("rb") as file:
            while True:
                data = file.read()
                if not data:
                    break
                yield data

    sep_t = bytes_stream_split_separator(separator=separator, keep_separator=keep_separator)
    return Stream(_inner()).transform(sep_t)


def from_file(
    path: str | Path,
    line_separator: bytes = b"\n",
    keep_separator: bool = False,
    encoding: str = "utf-8",
) -> Stream[str]:
    t_decode = partial(bytes.decode, encoding=encoding)
    return from_file_raw(path, line_separator, keep_separator).transform(t_decode)


__all__ = ("from_file", "from_file_raw", "from_stdin_raw")
