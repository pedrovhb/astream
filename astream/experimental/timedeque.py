from __future__ import annotations

import asyncio
import statistics
from datetime import datetime
import math
import random
from collections import deque
from datetime import timedelta
from itertools import chain, cycle
from statistics import mean
from typing import Generic, Iterator, Sequence, TypeVar

import rich
from loguru import logger
from rich.console import RenderResult, ConsoleOptions, Console, ConsoleRenderable, Group
from rich.live import Live
from rich.padding import Padding
from rich.panel import Panel
from rich.progress import TextColumn
from rich.segment import Segment
from rich.style import Style
from rich.table import Column
from rich.text import Text
from rich.tree import Tree

from astream import Stream, arange_delayed, stream
from astream import arange_delayed_sine

_T = TypeVar("_T")

HORIZONTAL_BAR = "─"
VERTICAL_BAR = "│"
LEFT_BAR = "┌"
RIGHT_BAR = "┐"
LEFT_RIGHT_BAR = "┬"
BOTTOM_LEFT_BAR = "└"
BOTTOM_RIGHT_BAR = "┘"
LEFT_BOTTOM_BAR = "├"
RIGHT_BOTTOM_BAR = "┤"
TOP_BOTTOM_BAR = "┼"


def vertical_bar(
    value: float,
    max_value: float = 100,
    min_value: float = 0,
    include_space: bool = False,
) -> str:
    """Return a vertical bar character based on the value.

    Args:
        value (float): The value to convert to a vertical bar.
        max_value (float, optional): The maximum value. Defaults to 100.
        min_value (float, optional): The minimum value. Defaults to 0.
        include_space (bool, optional): Whether to include a space before the bar. Defaults to False.

    Returns:
        str: A vertical bar character.
    """
    character_set = " ▁▂▃▄▅▆▇█" if include_space else "▁▂▃▄▅▆▇█"

    if value > max_value:
        value = max_value
    if value < min_value:
        value = min_value
    if max_value == min_value:
        return character_set[0]

    value = (value - min_value) / (max_value - min_value)
    index = math.ceil(value * (len(character_set) - 2))
    return character_set[index]


def vertical_bar_for_sequence(
    array: Sequence[float],
    max_value: float | None = None,
    min_value: float | None = None,
    include_space: bool = False,
) -> str:
    """Return a string of vertical bar characters based on the values in the array.

    Args:
        array (Sequence[float]): The array of values to convert to vertical bars.
        max_value (float, optional): The maximum value. Defaults to 100.
        min_value (float, optional): The minimum value. Defaults to 0.
        include_space (bool, optional): Whether to include a space before the bar. Defaults to False.

    Returns:
        str: A string of vertical bar characters.
    """
    # print(f"max_value: {max_value}, min_value: {min_value}")
    return "".join(vertical_bar(value, max_value, min_value, include_space) for value in array)


class TimeDeque(ConsoleRenderable, Generic[_T]):
    def __init__(self, n_bins: int, bin_size: timedelta | float) -> None:
        self.n_bins = n_bins
        self.bin_size = bin_size if isinstance(bin_size, timedelta) else timedelta(seconds=bin_size)

        loop = asyncio.get_event_loop()
        self.start_time = loop.time()
        self._iters = 0
        loop.call_at(
            self.start_time + (self._iters * self.bin_size.total_seconds() + 1), self._rotate
        )

        self._current_bin: list[_T] = []
        self._closed_bins: deque[list[_T]] = deque(maxlen=n_bins)
        self._bin_item_counts = deque((0 for _ in range(n_bins * 5)), maxlen=n_bins * 5)
        self._max_bin_item_count = 0
        self._moving_avg_item_count = 0.0

    def __len__(self) -> int:
        return self.n_bins

    def __getitem__(self, index: int) -> list[_T]:
        if index == 0:
            return self._current_bin
        elif index > 0:
            return self._closed_bins[index - 1]
        else:
            return self._closed_bins[index]

    def __iter__(self) -> Iterator[list[_T]]:
        return iter(chain((self._current_bin,), self._closed_bins))

    def _rotate(self) -> None:
        self._iters += 1
        loop = asyncio.get_event_loop()
        next_iter_delta = self._iters * self.bin_size.total_seconds() + 1
        loop.call_at(self.start_time + next_iter_delta, self._rotate)

        items_in_crt = len(self._current_bin)
        self._bin_item_counts.appendleft(items_in_crt)
        self._max_bin_item_count = max(self._bin_item_counts)
        self._closed_bins.appendleft(self._current_bin)
        self._current_bin = []

    def append(self, item: _T) -> None:
        self._current_bin.append(item)

    def __stream_map__(self, stream: Stream[_T]) -> Stream[_T]:
        def _mapper(item: _T) -> _T:
            self.append(item)
            return item

        s = stream / _mapper
        setattr(s, "_timedeque", self)
        return s

    @property
    def closed_bins(self) -> deque[list[_T]]:
        return self._closed_bins

    def rates(self) -> list[float]:
        """Return the rates of the bins in items/sec. The first bin is the most recent."""
        return [bin_count / self.bin_size.total_seconds() for bin_count in self._bin_item_counts]

    def visualize(self) -> str:
        """Return a string of vertical bar characters representing the rates of the bins."""

        if self._bin_item_counts:
            return vertical_bar_for_sequence(
                list(self._bin_item_counts)[: self.n_bins * 2],
                min_value=0,
                max_value=self._max_bin_item_count,
                include_space=True,
            )
        else:
            return ""
        # if not valid_rates:
        #     return ""
        # self._max_rate = max(self._max_rate, max(valid_rates))
        # self._min_rate = min(self._min_rate, min(valid_rates))
        # # print(f"max_rate: {self._max_rate}, min_rate: {self._min_rate}")
        # return vertical_bar_for_sequence(
        #     valid_rates,
        #     max_value=self._max_rate,
        #     min_value=self._min_rate - 1 if self._min_rate > 0 else 0,
        #     include_space=True,
        #     log_scale=True,
        # )

    # def get_items_per_second(self) -> float:
    #     """Return the rate of the most recent complete bin in items/sec."""
    #     # Exponential moving average, alpha = 0.5 with normalization
    #     acc = 0.0
    #     alpha = 0.5
    #     i = 0
    #
    #     for i, rate in enumerate(self.rates()):
    #         acc += (alpha**i) * rate
    #     return acc / (1 - (alpha ** (i + 1)))

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        # yield Segment(self.visualize(), style=Style(color="green"))
        # yield Segment(datetime.now().strftime("%H:%M:%S"), style=Style(color="green"))

        # Segment with down corner, horizontal line, then ┤

        # HORIZONTAL_BAR = "─"
        # VERTICAL_BAR = "│"
        # LEFT_BAR = "┌"
        # RIGHT_BAR = "┐"
        # LEFT_RIGHT_BAR = "┬"
        # BOTTOM_LEFT_BAR = "└"
        # BOTTOM_RIGHT_BAR = "┘"
        # LEFT_BOTTOM_BAR = "├"
        # RIGHT_BOTTOM_BAR = "┤"
        # TOP_BOTTOM_BAR = "┼"
        bar = self.visualize()
        mean_rate = mean(self.rates())
        width = min(options.max_width, len(bar))

        latest_item = (
            self._current_bin[-1]
            if self._current_bin
            else self._closed_bins[0][-1]
            if self._closed_bins
            else None
        )
        group = Group(
            f"[yellow bold]arange(100) / [red]add_two[/red] / stringify[/]",
            Text(bar, style=Style(color="green")),
            f"[white][bold]{mean_rate:.2f}[/][blue] items per second[/blue]"
            + f" (latest: {latest_item!r})",
        )

        panel = Panel(
            group,
            title=Text(datetime.now().strftime("%H:%M:%S"), style=Style(color="green")),
            border_style="blue",
            padding=(0, 1),
            width=width + 4,
        )
        # margin:
        tree = Tree(panel)
        for i, bin in enumerate(self):
            t = self.bin_size * (i + 1)
            t_str = f"{t.total_seconds():.2f} s ago"
            tree.add(f"[bold]{t_str:<12}[/] [bold]{len(bin)}[/] items")

        yield Padding(tree, (0, 2))

        # yield Segment("└─────┤", style=Style(color="blue"))
        # yield Segment(self.visualize(), style=Style(color="green"))
        # yield Segment("├ ", style=Style(color="blue"))
        # yield Segment(f" {self.rates()[1]:.2f} items/sec", style=Style(color="green"))


async def main() -> None:
    td = TimeDeque[int](20, 0.1)
    acycle = stream(cycle)
    console = Console()
    with Live(td, refresh_per_second=10, console=console) as live:
        while True:
            st = arange_delayed_sine(100000, delay_min=0.01, delay_max=0.08, rate=10) / td
            async for item in st:
                console.log(f"Got {item}")


"""
85 deque([deque([85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70]), deque([69, 68, 67, 66, 65])], maxlen=5)
86 deque([deque([86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70]), deque([69, 68, 67, 66, 65])], maxlen=5)
87 deque([deque([87, 86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70]), deque([69, 68, 67, 66, 65])], maxlen=5)
88 deque([deque([88, 87, 86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70]), deque([69, 68, 67, 66, 65])], maxlen=5)
89 deque([deque([89, 88, 87, 86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70]), deque([69, 68, 67, 66, 65])], maxlen=5)
2022-11-17 21:23:21.008 | INFO     | __main__:_rotate:51 - Rotated at 107589.436252607 (drift: 0.000537934000021778)
90 deque([deque([90]), deque([89, 88, 87, 86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70])], maxlen=5)
91 deque([deque([91, 90]), deque([89, 88, 87, 86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70])], maxlen=5)
92 deque([deque([92, 91, 90]), deque([89, 88, 87, 86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70])], maxlen=5)
93 deque([deque([93, 92, 91, 90]), deque([89, 88, 87, 86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70])], maxlen=5)
94 deque([deque([94, 93, 92, 91, 90]), deque([89, 88, 87, 86, 85]), deque([84, 83, 82, 81, 80]), deque([79, 78, 77, 76, 75]), deque([74, 73, 72, 71, 70])], maxlen=5)
2022-11-17 21:23:22.009 | INFO     | __main__:_rotate:51 - Rotated at 107590.436549587 (drift: 0.0008345730020664632)
"""

if __name__ == "__main__":
    asyncio.run(main())
