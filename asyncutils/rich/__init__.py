from __future__ import annotations

import random
from itertools import repeat
from typing import TypeVar, Generic

from rich import inspect
from rich.console import Console, RenderResult, ConsoleOptions
from rich.live import Live
from rich.segment import Segment
from rich.style import Style

from asyncutils.closeable_queue import CloseableQueue, QueueExhausted

con = Console()

_T = TypeVar("_T")


def _make_bar(value: int, max_value: int, width: int, style: Style) -> RenderResult:
    partial_bars = "▏▎▍▌▋▊▉"

    bar = Segment("█", style)
    space = Segment(" ", style)
    whole, partial = divmod((value / max_value) * width, 1)
    yield from (
        *repeat(bar, int(whole)),
        Segment(partial_bars[int(partial * len(partial_bars))], style),
        *repeat(space, width - int(whole) - 1),
    )


class RichCloseableQueue(CloseableQueue[_T]):
    def __init__(self, maxsize: int = 0) -> None:
        super().__init__(maxsize)

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        max_size: str = str(self.maxsize) if self.maxsize > 0 else "∞"
        yield Segment(f"[{self.qsize()}/{max_size}]", Style(color="blue"))
        yield Segment(" ")

        if self.is_exhausted:
            yield Segment(" ⏹  ", Style(color="red"))
        elif self.is_closed:
            yield Segment(" ⏹  ", Style(color="yellow"))
        else:
            yield Segment(" ▶  ", Style(color="green"))

        yield from _make_bar(
            self.qsize(),
            self.maxsize,
            options.max_width - 4,
            Style(color="blue", bold=True, bgcolor="red"),
        )


async def main() -> None:
    q = RichCloseableQueue[int](maxsize=800)
    TIME_MUL = 0.01
    MAX_FILL = 12000

    with Live(q, console=con, auto_refresh=True) as live:

        async def filler() -> None:
            for i in range(MAX_FILL):
                await asyncio.sleep(random.uniform(0, 5 * TIME_MUL))
                await q.put(i)

            print(f"Closing queue...")
            q.close()

        async def consumer() -> None:
            while True:
                await asyncio.sleep(random.uniform(3 * TIME_MUL, 10 * TIME_MUL))
                try:
                    n = await q.get()
                    q.task_done()
                except QueueExhausted:
                    return
                else:
                    con.print(n)

        asyncio.create_task(consumer())
        asyncio.create_task(filler())

        await consumer()

    con.print(q)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
