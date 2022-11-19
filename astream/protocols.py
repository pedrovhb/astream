from __future__ import annotations

import inspect
import itertools
from types import NotImplementedType
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Generator,
    Generic,
    Iterable,
    List,
    ParamSpec,
    Protocol,
    Type,
    TypeAlias,
    TypeVar,
    cast,
    overload,
    runtime_checkable,
)

from astream import StreamMappable, Stream, StreamFilterable, StreamFlatMappable
from astream.stream_utils import (
    aconcatenate,
    afilter,
    aflatmap,
    amap,
    amerge,
    arepeat,
    atee,
)
from astream.utils import ensure_async_iterator, ensure_coro_fn

_R = TypeVar("_R")


_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_U = TypeVar("_U")
_CoroT: TypeAlias = Coroutine[Any, Any, _T]


class StreamLike(Protocol[_T], AsyncIterable[_T]):
    @overload
    def __truediv__(self, other: StreamMappable[_T, _R]) -> Stream[_R]:
        ...

    @overload
    def __truediv__(self, other: Callable[[_T], _CoroT[_R]]) -> Stream[_R]:
        ...

    @overload
    def __truediv__(self, other: Callable[[_T], _R]) -> Stream[_R]:
        ...

    @overload
    def __truediv__(self, other: Any) -> Stream[_R] | NotImplementedType:
        ...

    def __truediv__(self, other: Any) -> Stream[_R]:

        if isinstance(other, StreamMappable):
            return other.__stream_map__(self)

        if not callable(other):
            return NotImplemented

        self_name = (
            getattr(self, "name", None)
            or getattr(self, "__name__", None)
            or getattr(self, "__qualname__", None)
            or (getattr(self.__class__, "__name__", None) if hasattr(self, "__class__") else None)
            or "<StreamLike>"
        )

        if hasattr(other, "name"):
            name = other.name
        elif hasattr(other, "__name__"):
            name = f"{self_name} / {other.__name__}"
        else:
            name = self_name + " / <unnamed>"
        return Stream(amap(other, self), name=name)

    @overload
    def __mod__(self, other: StreamFilterable[_T]) -> Stream[_T]:
        ...

    @overload
    def __mod__(self, other: Callable[[_T], _CoroT[bool]]) -> Stream[_T]:
        ...

    @overload
    def __mod__(self, other: Callable[[_T], bool]) -> Stream[_T]:
        ...

    @overload
    def __mod__(self, other: Any) -> Stream[_T] | NotImplementedType:
        ...

    def __mod__(self, other: Any) -> Stream[_T] | NotImplementedType:

        if isinstance(other, StreamFilterable):
            return other.__stream_filter__(self)

        if not callable(other):
            return NotImplemented

        return Stream(afilter(other, self))

    @overload
    def __floordiv__(
        self: StreamLike[Iterable[_U]] | StreamLike[AsyncIterable[_U]],
        other: StreamFlatMappable[_U, _R],
    ) -> StreamLike[_R]:
        ...

    @overload
    def __floordiv__(
        self: StreamLike[Iterable[_U]] | StreamLike[AsyncIterable[_U]],
        other: Callable[[_U], _CoroT[Iterable[_R]]],
    ) -> StreamLike[_R]:
        ...

    @overload
    def __floordiv__(
        self: StreamLike[Iterable[_U]] | StreamLike[AsyncIterable[_U]],
        other: Callable[[_U], Iterable[_R]],
    ) -> StreamLike[_R]:
        ...

    @overload
    def __floordiv__(
        self: StreamLike[Iterable[_U]] | StreamLike[AsyncIterable[_U]],
        other: Callable[[_U], AsyncIterable[_R]],
    ) -> StreamLike[_R]:
        ...

    @overload
    def __floordiv__(self, other: Any) -> StreamLike[_R] | NotImplementedType:
        ...

    def __floordiv__(
        self: StreamLike[Iterable[_U]], other: Any
    ) -> StreamLike[_R] | NotImplementedType:

        if isinstance(other, StreamFlatMappable):
            return other.__stream_flatmap__(self)

        if not callable(other):
            return NotImplemented
        return Stream(aflatmap(other, self))

    @overload
    def __pos__(self: StreamLike[List[_R]]) -> StreamLike[_R]:
        ...

    @overload
    def __pos__(self: StreamLike[Iterable[_R]]) -> StreamLike[_R]:
        ...

    @overload
    def __pos__(self: StreamLike[AsyncIterable[_R]]) -> StreamLike[_R]:
        ...

    @overload
    def __pos__(self: StreamLike[AsyncIterator[_R]]) -> StreamLike[_R]:
        ...

    def __pos__(self: StreamLike[AsyncIterable[_R]] | StreamLike[Iterable[_R]]) -> StreamLike[_R]:
        async def _flat() -> AsyncIterator[_R]:
            async for item in self:
                async for subitem in ensure_async_iterator(item):
                    yield subitem

        return Stream(_flat())

    @overload
    def __matmul__(self, other: Callable[[list[_T]], _CoroT[_R]]) -> _CoroT[_R]:
        ...

    @overload
    def __matmul__(self, other: Callable[[list[_T]], _R]) -> _CoroT[_R]:
        ...

    @overload
    def __matmul__(self, other: Any) -> _CoroT[_R] | NotImplementedType:
        ...

    def __matmul__(self, other: Any) -> _CoroT[_R] | NotImplementedType:
        if not callable(other):
            return NotImplemented

        async def _collect() -> _R:
            coro_fn = cast(Callable[[list[_T]], _CoroT[_R]], ensure_coro_fn(other))
            return await coro_fn([item async for item in self])

        return _collect()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._async_iterable!r})"

    def __str__(self) -> str:
        return f"{type(self).__name__}({self._async_iterable!s})"

    def __await__(self) -> Generator[None, None, list[_T]]:
        async def _collect() -> list[_T]:
            return [item async for item in self]

        return _collect().__await__()

    def __add__(self, other: Iterable[_U] | AsyncIterable[_U]) -> Stream[_T | _U]:
        """Concatenate two streams."""
        cls = cast(Type[Stream[_T | _U]], type(self))
        return cls(aconcatenate(self, other))

    def __radd__(self, other: Iterable[_U] | AsyncIterable[_U]) -> Stream[_T | _U]:
        """Concatenate two streams."""
        cls = cast(Type[Stream[_T | _U]], type(self))
        return cls(aconcatenate(other, self))

    def __mul__(self, other: int) -> Stream[_T]:
        """Concatenate stream with itself multiple times."""
        return Stream(arepeat(self, other))

    __rmul__ = __mul__

    def __pow__(self, other: Iterable[_U] | AsyncIterable[_U]) -> Stream[_T | _U]:
        """Merge a stream with another, yielding items from both as they arrive."""
        return Stream(amerge(self, other))

    __rpow__ = __pow__

    amap = __truediv__
    afilter = __mod__
    aflatmap = __floordiv__
    aflat = __pos__
    acollect = __matmul__
    agather = __await__
