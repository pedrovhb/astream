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


@runtime_checkable
class StreamMappable(Protocol[_T, _R]):
    def __stream_map__(self, stream: Stream[_T]) -> Stream[_R]:
        ...


@runtime_checkable
class StreamFilterable(Protocol[_T]):
    def __stream_filter__(self, stream: Stream[_T]) -> Stream[_T]:
        ...


@runtime_checkable
class StreamFlatMappable(Protocol[_T, _R]):
    def __stream_flatmap__(
        self, stream: Stream[Iterable[_T]] | Stream[AsyncIterable[_T]]
    ) -> Stream[_R]:
        ...


@runtime_checkable
class StreamCollector(Protocol[_T, _R]):
    def __stream_collect__(self, stream: Stream[_T]) -> Stream[_R]:
        ...


class Stream(AsyncIterator[_T], Generic[_T]):
    def __new__(
        cls,
        async_iterable: AsyncIterable[_T] | Iterable[_T],
        *args: Any,
        **kwargs: Any,
    ) -> Stream[_T]:
        if isinstance(async_iterable, cls):
            return async_iterable
        return super().__new__(cls)

    @overload
    def __init__(self, iterable: AsyncIterable[_T], name: str | None = ...) -> None:
        ...

    @overload
    def __init__(self, iterable: Iterable[_T] | AsyncIterable[_T], name: str | None = ...) -> None:
        ...

    def __init__(self, iterable: Iterable[_T] | AsyncIterable[_T], name: str | None = None) -> None:
        self._async_iterator = ensure_async_iterator(iterable)
        self._name = name if name is not None else f"<stream {self._default_name()}>"

    @property
    def name(self) -> str:
        return self._name

    def _default_name(self) -> str:
        return f"{hash(self).to_bytes(8, 'big').hex()}"

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        try:
            return await self._async_iterator.__anext__()
        except StopAsyncIteration:
            raise StopAsyncIteration

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

        cls = cast(Type[Stream[_R]], type(self))

        if hasattr(other, "name"):
            name = other.name
        elif hasattr(other, "__name__"):
            name = f"{self.name} / {other.__name__}"
        else:
            name = self.name + " / <unnamed>"
        return cls(amap(other, self), name=name)

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

        return type(self)(afilter(other, self))

    @overload
    def __floordiv__(
        self: Stream[Iterable[_U]] | Stream[AsyncIterable[_U]],
        other: StreamFlatMappable[_U, _R],
    ) -> Stream[_R]:
        ...

    @overload
    def __floordiv__(
        self: Stream[Iterable[_U]] | Stream[AsyncIterable[_U]],
        other: Callable[[_U], _CoroT[Iterable[_R]]],
    ) -> Stream[_R]:
        ...

    @overload
    def __floordiv__(
        self: Stream[Iterable[_U]] | Stream[AsyncIterable[_U]],
        other: Callable[[_U], Iterable[_R]],
    ) -> Stream[_R]:
        ...

    @overload
    def __floordiv__(
        self: Stream[Iterable[_U]] | Stream[AsyncIterable[_U]],
        other: Callable[[_U], AsyncIterable[_R]],
    ) -> Stream[_R]:
        ...

    @overload
    def __floordiv__(self, other: Any) -> Stream[_R] | NotImplementedType:
        ...

    def __floordiv__(self: Stream[Iterable[_U]], other: Any) -> Stream[_R] | NotImplementedType:

        if isinstance(other, StreamFlatMappable):
            return other.__stream_flatmap__(self)

        if not callable(other):
            return NotImplemented
        cls = cast(Type[Stream[_R]], type(self))
        return cls(aflatmap(other, self))

    @overload
    def __pos__(self: Stream[List[_R]]) -> Stream[_R]:
        ...

    @overload
    def __pos__(self: Stream[Iterable[_R]]) -> Stream[_R]:
        ...

    @overload
    def __pos__(self: Stream[AsyncIterable[_R]]) -> Stream[_R]:
        ...

    @overload
    def __pos__(self: Stream[AsyncIterator[_R]]) -> Stream[_R]:
        ...

    def __pos__(self: Stream[AsyncIterable[_R]] | Stream[Iterable[_R]]) -> Stream[_R]:
        async def _flat() -> AsyncIterator[_R]:
            async for item in self:
                async for subitem in ensure_async_iterator(item):
                    yield subitem

        return cast(Type[Stream[_R]], type(self))(_flat())

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
        return f"{type(self).__name__}({self._async_iterator!r})"

    def __str__(self) -> str:
        return f"{type(self).__name__}({self._async_iterator!s})"

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
        # todo - switch this out by the more useful starmap
        pass

    def __pow__(self, other: Iterable[_U] | AsyncIterable[_U]) -> Stream[_T | _U]:
        """Merge a stream with another, yielding items from both as they arrive."""
        cls = cast(Type[Stream[_T | _U]], type(self))
        return cls(amerge(self, other))

    __rpow__ = __pow__

    def aclone(self) -> Stream[_T]:
        a, b = atee(self._async_iterator, 2)
        self._async_iterator = a
        return type(self)(b)

    amap = __truediv__
    afilter = __mod__
    aflatmap = __floordiv__
    aflat = __pos__
    acollect = __matmul__
    agather = __await__


_P = ParamSpec("_P")


@overload
def stream(
    async_iterable_or_fn: AsyncIterable[_T] | Iterable[_T],
) -> Stream[_T]:
    ...


@overload
def stream(
    async_iterable_or_fn: Callable[_P, AsyncIterable[_T]],
) -> Callable[_P, Stream[_T]]:
    ...  # Takes an async iterable function


@overload
def stream(
    async_iterable_or_fn: Callable[_P, _CoroT[Iterable[_T]]],
) -> Callable[_P, _CoroT[Stream[_T]]]:
    ...  # Takes an async function returning an iterable


@overload
def stream(
    async_iterable_or_fn: Callable[_P, Iterable[_T]],
) -> Callable[_P, Stream[_T]]:
    ...  # Takes a function returning an iterable


def stream(
    async_iterable_or_fn: AsyncIterable[_T]
    | Iterable[_T]
    | Callable[_P, AsyncIterable[_T]]
    | Callable[_P, _CoroT[Iterable[_T]]]
    | Callable[_P, Iterable[_T]]
) -> Stream[_T] | Callable[_P, Stream[_T]] | Callable[_P, _CoroT[Stream[_T]]]:
    """Create a stream from an iterable or async iterable, or decorate a function to make it
    return a Stream.
    """

    if inspect.isasyncgenfunction(async_iterable_or_fn):
        _async_gen = async_iterable_or_fn

        def _asyncgen_stream(*args: _P.args, **kwargs: _P.kwargs) -> Stream[_T]:
            return Stream(_async_gen(*args, **kwargs))

        return _asyncgen_stream

    elif inspect.iscoroutinefunction(async_iterable_or_fn):
        _coro = async_iterable_or_fn

        async def _stream(*args: _P.args, **kwargs: _P.kwargs) -> Stream[_T]:
            return Stream(await _coro(*args, **kwargs))

        return _stream

    elif callable(async_iterable_or_fn):
        _fn = cast(Callable[_P, Iterable[_T]], async_iterable_or_fn)

        def _sync_stream(*args: _P.args, **kwargs: _P.kwargs) -> Stream[_T]:
            return Stream(_fn(*args, **kwargs))

        return _sync_stream

    return Stream(async_iterable_or_fn)


__all__ = (
    "Stream",
    "StreamMappable",
    "StreamFilterable",
    "StreamFlatMappable",
    "stream",
)
