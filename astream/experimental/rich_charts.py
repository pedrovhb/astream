from __future__ import annotations

import asyncio
import math
from collections import deque
from datetime import timedelta, datetime
from types import SimpleNamespace
from typing import Iterable, Collection, Generic, TypeVar, Sequence

import rich
from rich.color import Color
from rich.console import Console
from rich.console import ConsoleOptions, RenderResult, RenderableType
from rich.live import Live
from rich.measure import Measurement
from rich.padding import Padding
from rich.panel import Panel
from rich.segment import Segment
from rich.style import Style
from rich.text import Text

from astream import Stream, arange, arange_delayed

CHARACTERS_BAR_HOR = "▏▎▍▌▋▊▉█"
CHARACTERS_BAR_VER = "▁▂▃▄▅▆▇█"

_T = TypeVar("_T")
console = Console()


def normalize_values(
    values: Iterable[float],
    max_value: float,
    min_value: float,
) -> tuple[float, ...]:
    val_range = max_value - min_value
    if val_range == 0:
        return tuple(0 for _ in values)
    return tuple((value - min_value) / val_range for value in values)


def filled_area(
    values: Iterable[float],
    width: int,
    height: int,
    max_value: float | None = None,
    min_value: float | None = None,
) -> str:
    """Render a filled area chart of dimensions `width` x `height`."""
    if not values:
        return ""
    max_value = max(values) if max_value is None else max_value
    min_value = min(values) if min_value is None else min_value
    values = normalize_values(values, max_value, min_value)
    values = tuple(value * height for value in values)
    cols = []
    for i in range(min(width, len(values))):
        div, mod = divmod(values[i], 1)
        col = ["█"] * int(div)
        if mod:
            col.append(CHARACTERS_BAR_VER[int(mod * 8)])
        col = col + [" "] * (height - len(col))
        cols.append(col[::-1])

    characters = []

    for i in range(height):
        line_chars = []
        for j in range(min(width, len(values))):
            line_chars.append(cols[j][i])
        yield "".join(line_chars)


class TimeValues(Generic[_T], Sequence[_T]):
    def __len__(self) -> int:
        return len(self._values)

    def __init__(
        self,
        initial_value: _T,
        time_interval: timedelta | float,
        time_kept: timedelta | float,
    ) -> None:

        if isinstance(time_interval, timedelta):
            self._time_interval = time_interval.total_seconds()
        else:
            self._time_interval = time_interval

        if isinstance(time_kept, timedelta):
            self._time_kept = time_kept.total_seconds()
        else:
            self._time_kept = time_kept

        n_items = int(self._time_kept / self._time_interval)
        self._values: deque[_T] = deque((initial_value for _ in range(n_items)), maxlen=n_items)

        self._start_time = asyncio.get_running_loop().time()
        self._n_iters = 1
        asyncio.get_running_loop().call_later(self._time_interval, self._rotate)

        self._current_value = initial_value

    def _rotate(self) -> None:
        self._n_iters += 1
        # rich.print(f"rotate: {self._current_value}")
        # next_t = self._start_time + self._time_interval * self._n_iters
        asyncio.get_running_loop().call_later(self._time_interval, self._rotate)
        self._values.appendleft(self._current_value)

    def set_value(self, value: _T) -> None:
        self._current_value = value

    @classmethod
    async def from_stream(
        cls,
        stream: Stream[_T],
        time_interval: timedelta | float,
        time_kept: timedelta | float,
        clone_stream: bool = True,
    ) -> TimeValues[_T]:
        initial = await anext(stream)
        time_values = cls(initial, time_interval, time_kept)

        st = stream.aclone() if clone_stream else stream

        async def set_value() -> None:
            async for value in st:
                time_values.set_value(value)

        asyncio.create_task(set_value())
        return time_values

    def __getitem__(self, index: timedelta | datetime | float | int) -> _T:
        # if isinstance(index, datetime):
        #     dt = index - datetime.now()
        # elif isinstance(index, timedelta):
        #     dt = index
        # elif isinstance(index, (float, int)):
        #     dt = timedelta(seconds=index)
        # else:
        #     raise TypeError(f"Invalid index type: {type(index)}")
        #
        # if dt > timedelta():
        #     raise IndexError("Index out of range")
        #
        # index = int(dt.total_seconds() / self._time_interval)
        # todo - interpolate
        return self._values[index]

    def __bool__(self) -> bool:
        return bool(self._values)


class FilledAreaChart:

    # COLOR_BELOW_ZERO is indigo
    COLOR_BELOW_ZERO = Color.from_rgb(75, 50, 100)
    COLOR_ABOVE_ZERO = Color.from_rgb(80, 60, 110)

    def __init__(
        self,
        data: Collection[float],
        height: int = 10,
        max_y: float | None = None,
        min_y: float | None = None,
    ) -> None:
        self.data = data
        self.max_y = max_y
        self.min_y = min_y
        self.height = height

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        if not self.data:
            yield Segment("No data")
            return
        area = filled_area(
            self.data,
            min(len(self.data), options.max_width),
            height=self.height,
            max_value=self.max_y,
            min_value=self.min_y,
        )
        # area_seg = Text(area, style=Style(color="green", bgcolor="bright_black"))
        # padded = Padding(area_seg, (1, 2, 1, 2), style=Style(bgcolor="black"), expand=False)
        # panel = Panel(area_seg, padding=(1, 2, 1, 2), style=Style(bgcolor="black"), expand=False)
        # yield padded

        def chart_area(console, options):
            for i, line in enumerate(area):
                if i < self.height // 2:
                    yield Segment(
                        line,
                        style=Style(
                            color="green",
                            bgcolor=self.COLOR_BELOW_ZERO,
                            dim=True,
                        ),
                    )
                else:
                    yield Segment(line, style=Style(color="green", bgcolor=self.COLOR_ABOVE_ZERO))

                yield Segment.line()

        ns = SimpleNamespace(__rich_console__=chart_area)

        yield Padding(ns, (1, 2, 1, 2), style=Style(bgcolor="black"), expand=False)

    # def __rich_measure__(self, console: Console, max_width: int) -> Measurement:
    #     return Measurement.get(console, self.data, max_width=max_width)

    def __rich__(self) -> RenderableType:
        return self

    def __repr__(self) -> str:
        return f"FilledAreaChart({self.data!r})"


async def main() -> None:

    astream = (
        arange_delayed(0, 100000, delay=0.01)
        / (lambda x: x / 2)
        / (lambda x: math.sin(x / 40) * 4 + math.cos(x / 80) ** 2 + math.sin(x / 15) * 2)
        / (lambda x: x / 10)
    )

    time_values = await TimeValues.from_stream(astream, 0.1, 10)
    chart = FilledAreaChart(time_values, max_y=1, min_y=-1)
    with Live(chart, refresh_per_second=10, console=console) as live:
        i = 0
        async for value in astream:
            console.print(f"i: {value}")
            # data.set_value(math.sin(i / 100) ** 3 + math.cos(i / 80) + math.sin(i / 50))
            # if data:
            #     console.print(data[0])


if __name__ == "__main__":
    asyncio.run(main())
