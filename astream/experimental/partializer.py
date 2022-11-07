from __future__ import annotations

from functools import partial
from typing import Generic, Callable, Iterable, TypeVar, ParamSpec

from astream import Stream

_T = TypeVar("_T")
_U = TypeVar("_U")
_P = ParamSpec("_P")


class F(Generic[_P, _U]):
    def __init__(self, fn: Callable[_P, _U]) -> None:
        self.fn = fn

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> Callable[_P, _U]:
        return partial(self.fn, *args, **kwargs)

    def __rtruediv__(self, other):
        if isinstance(other, Stream):
            return other.amap(self.fn)
        return NotImplemented

    def __rfloordiv__(self, other: Stream[Iterable[_T]]) -> Stream[_T]:
        if isinstance(other, Stream):
            return other.aflatmap(self.fn)
        return NotImplemented
