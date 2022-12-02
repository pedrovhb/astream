from __future__ import annotations

from abc import ABC
from functools import wraps, singledispatch
from types import NotImplementedType
from typing import *

from .utils import ensure_coroutine_function, ensure_async_iterator

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)

_A = TypeVar("_A")
_B = TypeVar("_B")
_C = TypeVar("_C")
_D = TypeVar("_D")
_E = TypeVar("_E")

_U = TypeVar("_U")

_I = TypeVar("_I", contravariant=True)
_O = TypeVar("_O", covariant=True)
_R = TypeVar("_R")

_IP = TypeVar("_IP")
_OP = TypeVar("_OP")

_P = ParamSpec("_P")

CoroFn: TypeAlias = Callable[_P, Coroutine[object, object, _T_co]]
SyncFn: TypeAlias = Callable[_P, _T_co]
EitherFn: TypeAlias = Union[CoroFn[_P, _T_co], SyncFn[_P, _T_co]]
EitherIterable: TypeAlias = Union[Iterable[_T_co], AsyncIterable[_T_co]]


# Out of ideas ¯\_(ツ)_/¯
# Typing with just Iterable and AsyncIterable is not enough, mypy says
# `incompatible self-type ...` for __pos__
if TYPE_CHECKING:
    SomeIterable: TypeAlias = Union[
        "Stream[Iterable[_T]]",
        "Stream[AsyncIterable[_T]]",
        "Stream[Iterator[_T]]",
        "Stream[AsyncIterator[_T]]",
        "Stream['Stream[_T]']",
        "Stream[list[_T]]",
        "Stream[Sequence[_T]]",
        "Stream[set[_T]]",
        "Stream[frozenset[_T]]",
        "Stream[tuple[_T, ...]]",
        "Stream[tuple[_T]]",
        "Stream[tuple[_T, _T]]",
        "Stream[tuple[_T, _T, _T]]",
        "Stream[tuple[_T, _T, _T, _T]]",
        "Stream[tuple[_T, _T, _T, _T, _T]]",
        "Stream[AsyncGenerator[_T, None]]",
        "Stream[Generator[_T, None, None]]",
        "Stream[Sequence[_T]]",
        "Stream[MutableSequence[_T]]",
        "Stream[Collection[_T]]",
        "Stream[Reversible[_T]]",
        "Stream[ValuesView[_T]]",
        "Stream[AbstractSet[_T]]",
        "Stream[MutableSet[_T]]",
        "Stream[KeysView[_T]]",
        "Stream[ValuesView[_T]]",
        "Stream[Deque[_T]]",
    ]
    FlattenSignatureT: TypeAlias = Callable[["SomeIterable[_U]"], "Stream[_U]"]


class Transformer(Generic[_I, _O], ABC):
    def transform(self, src: AsyncIterable[_I]) -> AsyncIterator[_O]:
        raise NotImplementedError

    ####################################################################
    # __truediv__ (a.k.a. `/`) overloads
    # Apply Transformer
    # - Transformer     /  Transformer     -> TransformerPipeline
    # - Transformer     /  Callable        -> Transformer @ Map(Callable)     -> TransformerPipeline
    # - Callable        /  Transformer     -> Map(Callable) @ Transformer     -> TransformerPipeline
    # - Async?Iterable  /  Transformer     -> Stream @ Transformer            -> Stream

    @overload
    def __truediv__(self, other: Transformer[_O, _R]) -> TransformerPipeline[_I, _R]:
        ...

    @overload
    def __truediv__(self, other: CoroFn[[_O], _R]) -> TransformerPipeline[_I, _R]:
        ...

    @overload
    def __truediv__(self, other: SyncFn[[_O], _R]) -> TransformerPipeline[_I, _R]:
        ...

    @overload
    def __truediv__(self, other: object) -> TransformerPipeline[_I, _R] | NotImplementedType:
        ...

    def __truediv__(
        self,
        other: Transformer[_O, _R] | CoroFn[[_O], _R] | SyncFn[[_O], _R] | object,
    ) -> TransformerPipeline[_I, _R] | NotImplementedType:

        if isinstance(other, type) and issubclass(other, Transformer):
            _other = cast(type[Transformer[_O, _R]], other)
            return TransformerPipeline(self, _other())

        # Transformer / Transformer -> TransformerPipeline
        if isinstance(other, Transformer):
            return TransformerPipeline(self, other)

        # Transformer / Callable -> Transformer / Map(Callable) -> TransformerPipeline
        if callable(other):
            return TransformerPipeline(self, Map(other))

        return NotImplemented

    @overload
    def __rtruediv__(self, other: Iterable[_I]) -> Stream[_O]:
        ...

    @overload
    def __rtruediv__(self, other: AsyncIterable[_I]) -> Stream[_O]:
        ...

    @overload
    def __rtruediv__(self, other: CoroFn[[_T], _I]) -> TransformerPipeline[_T, _O]:
        ...

    @overload
    def __rtruediv__(self, other: SyncFn[[_T], _I]) -> TransformerPipeline[_T, _O]:
        ...

    @overload
    def __rtruediv__(
        self, other: object
    ) -> Stream[_O] | TransformerPipeline[_T, _O] | NotImplementedType:
        ...

    def __rtruediv__(
        self,
        other: AsyncIterable[_I] | Iterable[_I] | CoroFn[[_T], _I] | SyncFn[[_T], _I] | object,
    ) -> Stream[_O] | TransformerPipeline[_T, _O] | NotImplementedType:

        # AsyncIterable / Transformer -> Stream @ Transformer -> Stream
        if isinstance(other, (AsyncIterable, Iterable)):
            return Stream(other).transform(self)

        # Callable / Transformer -> Map(Callable) @ Transformer -> TransformerPipeline
        if callable(other):
            return TransformerPipeline(Map(other), self)

        return NotImplemented

    ####################################################################
    # __floordiv__ (a.k.a. `//`) overloads
    # Flat map
    # - Transformer     //  Transformer     -> TransformerPipeline
    # - Transformer     //  Callable        -> Transformer @ FlatMap(Callable) -> TransformerPipeline
    # - Callable        //  Transformer     -> FlatMap(Callable) @ Transformer -> TransformerPipeline

    @overload
    def __floordiv__(self, other: FlatMap[_O, _R]) -> TransformerPipeline[_I, _R]:
        ...

    @overload
    def __floordiv__(self, other: CoroFn[[_O], Iterable[_R]]) -> TransformerPipeline[_I, _R]:
        ...

    @overload
    def __floordiv__(self, other: SyncFn[[_O], Iterable[_R]]) -> TransformerPipeline[_I, _R]:
        ...

    @overload
    def __floordiv__(self, other: object) -> TransformerPipeline[_I, _R] | NotImplementedType:
        ...

    def __floordiv__(
        self,
        other: FlatMap[_O, _R] | CoroFn[[_O], Iterable[_R]] | SyncFn[[_O], Iterable[_R]] | object,
    ) -> TransformerPipeline[_I, _R] | NotImplementedType:

        if not callable(other) and not isinstance(other, FlatMap):
            return NotImplemented

        # Transformer // Callable -> Transformer @ FlatMap(Callable) -> TransformerPipeline
        if callable(other):
            other = FlatMap(other)
        return TransformerPipeline(self, other)

    ####################################################################
    # __mod__ (a.k.a. `%`) overloads
    # Filter
    # - Transformer     %  Callable        -> Transformer @ Filter(Callable)  -> TransformerPipeline
    # - Callable        %  Transformer     -> Filter(Callable) @ Transformer  -> TransformerPipeline
    # Note: AsyncIterable % Transformer is not defined

    @overload
    def __mod__(self, other: CoroFn[[_O], bool]) -> TransformerPipeline[_I, _O]:
        ...

    @overload
    def __mod__(self, other: SyncFn[[_O], bool]) -> TransformerPipeline[_I, _O]:
        ...

    @overload
    def __mod__(self, other: object) -> TransformerPipeline[_I, _O] | NotImplementedType:
        ...

    def __mod__(
        self, other: CoroFn[[_O], bool] | SyncFn[[_O], bool] | object
    ) -> TransformerPipeline[_I, _O] | NotImplementedType:
        # Transformer % Callable -> Transformer @ Filter(Callable) -> TransformerPipeline
        if callable(other):
            return TransformerPipeline(self, Filter(other))
        return NotImplemented

    @overload
    def __rmod__(self, other: CoroFn[[_I], bool]) -> TransformerPipeline[_I, _O]:
        ...

    @overload
    def __rmod__(self, other: SyncFn[[_I], bool]) -> TransformerPipeline[_I, _O]:
        ...

    @overload
    def __rmod__(self, other: object) -> TransformerPipeline[_I, _O] | NotImplementedType:
        ...

    def __rmod__(
        self, other: SyncFn[[_I], bool] | CoroFn[[_I], bool] | object
    ) -> TransformerPipeline[_I, _O] | NotImplementedType:
        # Callable % Transformer -> Filter(Callable) @ Transformer -> TransformerPipeline

        if callable(other):
            return TransformerPipeline(Filter(other), self)

        return NotImplemented

    ####################################################################
    # __rrshift__ (a.k.a. `O >> T`) and __lshift__ (a.k.a. T << O) overloads
    # Async?Iterable >> Transformer -> Stream @ Transformer -> Stream
    # Transformer << Async?Iterable -> Stream @ Transformer -> Stream

    @overload
    def __rrshift__(self, other: EitherIterable[_I]) -> Stream[_O]:
        ...

    @overload
    def __rrshift__(self, other: object) -> Stream[_O] | NotImplementedType:
        ...

    def __rrshift__(self, other: EitherIterable[_I] | object) -> Stream[_O] | NotImplementedType:
        # AsyncIterable >> Transformer -> Stream @ Transformer -> Stream
        if isinstance(other, (AsyncIterable, Iterable)):
            return Stream(other).transform(self)
        return NotImplemented

    @overload
    def __lshift__(self, other: EitherIterable[_I]) -> Stream[_O]:
        ...

    @overload
    def __lshift__(self, other: object) -> Stream[_O] | NotImplementedType:
        ...

    def __lshift__(self, other: EitherIterable[_I] | object) -> Stream[_O] | NotImplementedType:
        # Transformer << AsyncIterable -> Stream @ Transformer -> Stream
        if isinstance(other, (AsyncIterable, Iterable)):
            return Stream(other).transform(self)
        return NotImplemented

    # todo - __lshift__ (should be the same as __rtruediv__ but with the arguments reversed)


class Stream(AsyncIterator[_T]):
    def __init__(self, src: EitherIterable[_T]) -> None:
        self._src = ensure_async_iterator(src)

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        return await self._src.__anext__()

    def transform(self, transformer: Transformer[_T, _R]) -> Stream[_R]:
        cls_ = cast(Type[Stream[_R]], type(self))
        return cls_(transformer.transform(self))

    @overload
    def __truediv__(self, other: Transformer[_T, _R]) -> Stream[_R]:
        ...

    @overload
    def __truediv__(self, other: CoroFn[[_T], _R]) -> Stream[_R]:
        ...

    @overload
    def __truediv__(self, other: SyncFn[[_T], _R]) -> Stream[_R]:
        ...

    @overload
    def __truediv__(self, other: object) -> Stream[_R] | NotImplementedType:
        ...

    def __truediv__(
        self, other: Transformer[_T, _R] | SyncFn[[_T], _R] | CoroFn[[_T], _R] | object
    ) -> Stream[_R] | NotImplementedType:
        """Map the stream using the given function or transformer."""

        # Stream / Transformer -> Stream @ Transformer -> Stream
        if _no_param_transformer := getattr(other, "_no_param_transformer", None):
            # Allow for `Stream / Transformer` syntax when the given transformer can be
            # instantiated with no arguments, e.g. `Stream / pairwise` vs. `Stream / pairwise()`
            _no_param_transformer = cast(type[Transformer[_T, _R]], other)
            return self.transform(_no_param_transformer())

        # Stream / Transformer -> Stream @ Transformer -> Stream
        if isinstance(other, Transformer):
            return self.transform(other)

        # Stream / Callable -> Map(Callable) @ Stream -> Stream
        if callable(other):
            return self.transform(Map(other))

        return NotImplemented

    @overload
    def __floordiv__(self, other: CoroFn[[_T], Iterable[_R]]) -> Stream[_R]:
        ...

    @overload
    def __floordiv__(self, other: CoroFn[[_T], AsyncIterable[_R]]) -> Stream[_R]:
        ...

    @overload
    def __floordiv__(self, other: SyncFn[[_T], Iterable[_R]]) -> Stream[_R]:
        ...

    @overload
    def __floordiv__(self, other: SyncFn[[_T], AsyncIterable[_R]]) -> Stream[_R]:
        ...

    def __floordiv__(
        self,
        other: SyncFn[[_T], Iterable[_R]]
        | CoroFn[[_T], Iterable[_R]]
        | SyncFn[[_T], AsyncIterable[_R]]
        | CoroFn[[_T], AsyncIterable[_R]],
    ) -> Stream[_R]:
        """Flatten the stream using the given transformer."""
        return self.transform(FlatMap(other))

    @overload
    def __mod__(self, other: CoroFn[[_T], bool]) -> Stream[_T]:
        ...

    @overload
    def __mod__(self, other: SyncFn[[_T], bool]) -> Stream[_T]:
        ...

    @overload
    def __mod__(self, other: object) -> Stream[_T] | NotImplementedType:
        ...

    def __mod__(
        self, other: CoroFn[[_T], bool] | SyncFn[[_T], bool] | object
    ) -> Stream[_T] | NotImplementedType:
        """Filter the stream using the given function or async function as predicate."""
        # Stream % Callable -> Stream @ Filter(Callable) -> Stream
        if callable(other):
            return self.transform(Filter(other))
        return NotImplemented

    def __pos__(self: SomeIterable[_T]) -> Stream[_T]:
        """Flatten the stream."""
        return self.transform(FlatMap(lambda x: x))


class Map(Transformer[_I, _O]):
    @overload
    def __init__(self, fn: CoroFn[[_I], _O]) -> None:
        ...

    @overload
    def __init__(self, fn: SyncFn[[_I], _O]) -> None:
        ...

    def __init__(self, fn: CoroFn[[_I], _O] | SyncFn[[_I], _O]) -> None:
        self._fn = cast(Callable[[_I], Coroutine[Any, Any, _O]], ensure_coroutine_function(fn))

    async def transform(self, src: AsyncIterable[_I]) -> AsyncIterator[_O]:
        async for item in src:
            yield await self._fn(item)


class FlatMap(Transformer[_I, _O]):
    @overload
    def __init__(self, fn: CoroFn[[_I], Iterable[_O]]) -> None:
        ...

    @overload
    def __init__(self, fn: CoroFn[[_I], AsyncIterable[_O]]) -> None:
        ...

    @overload
    def __init__(self, fn: SyncFn[[_I], Iterable[_O]]) -> None:
        ...

    @overload
    def __init__(self, fn: SyncFn[[_I], AsyncIterable[_O]]) -> None:
        ...

    def __init__(
        self,
        fn: CoroFn[[_I], Iterable[_O]]
        | CoroFn[[_I], AsyncIterable[_O]]
        | SyncFn[[_I], Iterable[_O]]
        | SyncFn[[_I], AsyncIterable[_O]],
    ) -> None:
        self._fn = cast(
            Callable[[_I], Coroutine[Any, Any, Iterable[_O] | AsyncIterable[_O]]],
            ensure_coroutine_function(fn),
        )

    async def transform(self, src: AsyncIterable[_I]) -> AsyncIterator[_O]:
        async for item in src:
            async for sub_item in ensure_async_iterator(await self._fn(item)):
                yield sub_item


class Filter(Transformer[_T, _T]):
    @overload
    def __init__(self, fn: CoroFn[[_T], bool]) -> None:
        ...

    @overload
    def __init__(self, fn: SyncFn[[_T], bool]) -> None:
        ...

    def __init__(self, fn: CoroFn[[_T], bool] | SyncFn[[_T], bool]) -> None:
        self._fn = ensure_coroutine_function(fn)

    async def transform(self, src: AsyncIterable[_T]) -> AsyncIterator[_T]:
        async for item in src:
            if await self._fn(item):
                yield item


class TransformerPipeline(Transformer[_I, _O]):
    def __init__(self, t_a: Transformer[_I, _U], t_b: Transformer[_U, _O]) -> None:
        self._t_a = t_a
        self._t_b = t_b

    async def transform(self, src: AsyncIterable[_I]) -> AsyncIterator[_O]:
        t1 = self._t_a.transform(src)
        t2 = self._t_b.transform(t1)
        async for item in t2:
            yield item


class FnTransformer(Transformer[_I, _O]):
    def __init__(self, fn: Callable[[AsyncIterator[_I]], AsyncIterator[_O]]) -> None:
        self._fn = fn

    async def transform(self, src: EitherIterable[_I]) -> AsyncIterator[_O]:
        # todo - accept EitherIterable for any transform?
        src = ensure_async_iterator(src)
        async for item in self._fn(src):
            yield item


def transformer(
    _fn: Callable[Concatenate[AsyncIterator[_I], _P], AsyncIterator[_O]]
) -> Callable[_P, FnTransformer[_I, _O]]:
    @wraps(_fn)
    def _outer(*__args: _P.args, **__kwargs: _P.kwargs) -> FnTransformer[_I, _O]:
        def _inner(_src: AsyncIterator[_I]) -> AsyncIterator[_O]:
            return _fn(_src, *__args, **__kwargs)

        return FnTransformer(_inner)

    return _outer


def stream(__fn: Callable[_P, AsyncIterable[_O] | Iterable[_O]]) -> Callable[_P, Stream[_O]]:
    @wraps(__fn)
    def _outer(*__args: _P.args, **__kwargs: _P.kwargs) -> Stream[_O]:
        return Stream(ensure_async_iterator(__fn(*__args, **__kwargs)))

    return _outer


__all__ = [
    "Stream",
    "Transformer",
    "Map",
    "FlatMap",
    "Filter",
    "TransformerPipeline",
    "FnTransformer",
    "transformer",
    "stream",
]
