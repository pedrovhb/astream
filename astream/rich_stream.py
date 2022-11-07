from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TypeVar, Iterable, AsyncIterable

from rich.segment import Segment
from rich.console import Console, ConsoleOptions, RenderResult
from rich.live import Live

from astream import Stream, NoValueSentinel, SentinelType, arange
from astream.stream_utils import arange_delayed

_T = TypeVar("_T")

vertical_bar_characters = "▁▂▃▄▅▆▇█"


class RichStream(Stream[_T]):

    _latest_value: SentinelType | _T = NoValueSentinel
    _yielded_timestamps: list[datetime]

    def __init__(
        self,
        iterable: AsyncIterable[_T] | Iterable[_T],
        max_out_q_size: int = 0,
        start: bool = True,
    ):
        super().__init__(iterable, max_out_q_size, start)
        self._yielded_timestamps = []

    async def __anext__(self) -> _T:
        result = await super().__anext__()
        self._latest_value = result
        return result

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield Segment(f"Latest value: ")
        yield str(self._latest_value)
        yield f" (at {self._yielded_timestamps[-1].ctime()})" if self._yielded_timestamps else ""


if __name__ == "__main__":

    async def main() -> None:

        console = Console()
        st = RichStream(arange_delayed(100))

        with Live(st, console=console) as live:
            async for item in st:
                pass

    asyncio.run(main())
