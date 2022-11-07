from __future__ import annotations

from functools import partial
from typing import Generic, Callable, Iterable, TypeVar, ParamSpec, cast, Any

from astream import Stream
from astream.stream import WithStream

_T = TypeVar("_T")
_U = TypeVar("_U")
_P = ParamSpec("_P")


class F(Generic[_T, _U]):
    def __init__(self, fn: Callable[[_T], _U]) -> None:
        self.fn = fn

    def __call__(self, *args: Any, **kwargs: Any) -> WithStream[_T, _U]:
        part = partial(self.fn, *args, **kwargs)
        setattr(part, "__with_stream__", lambda stream: stream.amap(part))
        return cast(WithStream[_T, _U], part)
