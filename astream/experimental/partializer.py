from __future__ import annotations

from functools import partial
from typing import Generic, Callable, Iterable, TypeVar, ParamSpec, cast, Any

from astream import Stream
from astream.stream import StreamMapper, StreamFilterer, StreamFlatMapper

_T = TypeVar("_T")
_U = TypeVar("_U")
_P = ParamSpec("_P")


class F(Generic[_T, _U]):
    partial: Callable[[_T], _U]

    def __init__(self, fn: Callable[..., _U]) -> None:
        self.fn = fn
        self.partial = fn

    def __call__(self, *args: Any, **kwargs: Any) -> F[_T, _U]:
        self.partial = partial(self.fn, *args, **kwargs)
        # setattr(part, "__with_stream__", lambda stream: stream.amap(part))
        return self

    def __stream_map__(self, stream: Stream[_T]) -> Stream[_U]:
        return stream.amap(self.partial)

    def __stream_filter__(self: F[_T, bool], stream: Stream[_T]) -> Stream[_T]:
        return stream.afilter(self.partial)

    def __stream_flatmap__(self, stream: Stream[_T]) -> Stream[_U]:
        return stream.aflatmap(self.partial)
