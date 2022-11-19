from __future__ import annotations

import asyncio
import random
import statistics
from collections import deque
from datetime import datetime, timedelta
from functools import partial
from itertools import pairwise
from typing import AsyncIterable, Callable, Generic, Iterable, TypeVar

from humanize import naturaldelta, precisedelta
from rich.console import Console, ConsoleOptions, RenderResult
from rich.live import Live
from rich.segment import Segment

from astream import NoValue, SentinelType, Stream, arange, iter_to_aiter
from astream.stream_grouper import agroup_map
from astream.stream_utils import aenumerate, amerge, arange_delayed

try:
    import uvloop

    uvloop.install()
    print("Using uvloop")
except ImportError:
    print("Using asyncio")

_T = TypeVar("_T")

vertical_bar_characters = "▁▂▃▄▅▆▇█"


class RichStream(Stream[_T]):

    _latest_value: SentinelType | _T = NoValue
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


class StatsInstrumenter(Generic[_T]):

    _latest_value: SentinelType | _T = NoValue
    _yielded_timestamps: list[datetime]

    def __init__(self) -> None:
        self._yielded_timestamps = deque()

    def register_item(self, item: _T) -> None:
        self._latest_value = item
        self._yielded_timestamps.append(datetime.now())
        asyncio.get_event_loop().call_later(1, self._yielded_timestamps.popleft)

    # @property
    # def values_per_second(self) -> float:
    #     if len(self._yielded_timestamps) < 2:
    #         return 0.0
    #     now = datetime.now()
    #     ts_in_last_sec = sum((now - ts).total_seconds() for ts in self._yielded_timestamps) / len(
    #         self._yielded_timestamps
    #     )
    #     return ts_in_last_sec

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield Segment(f"Latest value: ")
        yield str(self._latest_value).strip()
        last = self._yielded_timestamps[-1] - datetime.now() if self._yielded_timestamps else None
        yield f" ({self.values_per_second:.1f}/s)" if last is not None else ""


def instrumenter(si: StatsInstrumenter[_T]) -> Callable[[_T], _T]:
    def _instrumenter(item: _T) -> _T:
        si.register_item(item)
        return item

    return _instrumenter


if __name__ == "__main__":

    async def main() -> None:

        console = Console()
        # st = RichStream(arange_delayed(100))
        si = StatsInstrumenter[int]()

        async def rand() -> Iterable[tuple[float, float]]:
            while True:
                yield random.uniform(-1, 1), random.uniform(-1, 1)
                await asyncio.sleep(0)

        async def is_in_circle(xy: tuple[float, float]) -> bool:
            x, y = xy
            return x**2 + y**2 < 1

        total = 0
        dentro = 0

        async def contar_in_circle(total_dentro) -> float:
            nonlocal total, dentro
            novo_total, novo_dentro = total_dentro
            total += novo_total
            dentro += novo_dentro
            return 4.0 * dentro / total

        # with Live(si, console=console, refresh_per_second=4) as live:
        st = (
            Stream(rand())
            / agroup_map(
                is_in_circle,
                {
                    True: lambda x: (1, 1),
                    False: lambda x: (1, 0),
                },
            )
            / contar_in_circle
        )
        start = datetime.now()
        async for i, item in aenumerate(st):
            if i % 10000 == 0:
                console.print(f"pi = {item:.12f} (after {i} iterations)")
                per_iter_dt = (datetime.now() - start) / (i or 1)
                per_iter = precisedelta(per_iter_dt, minimum_unit="microseconds")
                per_second = i / (datetime.now() - start).total_seconds()
                console.print(
                    f"Elapsed: {naturaldelta(datetime.now() - start)} "
                    f"(per iteration: {per_iter}, "
                    f"iterations per second: {per_second})"
                )

    asyncio.run(main())
