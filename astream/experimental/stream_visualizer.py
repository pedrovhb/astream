from __future__ import annotations

from typing import Generic, TypeVar, Sequence

from rich.console import Console, ConsoleRenderable, RenderResult, ConsoleOptions
from rich.segment import Segment

from astream import Stream, arange_delayed
from astream.experimental import TimeDeque

_T = TypeVar("_T")





class StreamNodeViz(Generic[_T], ConsoleRenderable):
    def __init__(self, stream: Stream[_T]) -> None:
        self.rate_deque = TimeDeque[float](10, 1)
        self.stream = stream / self.rate_deque

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        yield Segment("Stream: ")
        yield Segment(self.stream.name)
        yield Segment(
