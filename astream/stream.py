from __future__ import annotations

import asyncio
from functools import wraps
from types import NotImplementedType
from typing import *

from astream.pure import aflatten
from astream.sentinel import _NoValueT, Sentinel
from astream.utils import ensure_async_iterator, ensure_coroutine_function
from closeable_queue import CloseableQueue

# from worker_q import WorkerQueue

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
EitherIterable: TypeAlias = Union[Iterator[_T_co], AsyncIterator[_T_co]]

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


def _identity(x: _T) -> _T:
    return x


class Transformer(Generic[_I, _O]):
    def transform(self, src: AsyncIterator[_I]) -> AsyncIterator[_O]:
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
    # __mul__ (a.k.a. `**`) overloads
    def __pow__(self, n_workers: int) -> FnTransformer[_I, _O]:
        active_workers = n_workers

        out_queue: CloseableQueue[_O] = CloseableQueue()
        in_queue: CloseableQueue[_I] = CloseableQueue()

        async def worker() -> None:
            nonlocal active_workers
            async for item in self.transform(aiter(in_queue)):
                await out_queue.put(item)
            active_workers -= 1
            if active_workers == 0:
                out_queue.close()

        def _run(src: AsyncIterator[_I]) -> Stream[_O]:
            async def _inner() -> None:
                async for item in src:
                    await in_queue.put(item)
                in_queue.close()

            task_workers = [asyncio.create_task(worker()) for _ in range(n_workers)]
            task = asyncio.create_task(_inner())
            return Stream(out_queue)

        return FnTransformer(_run)

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


async def _consume(async_iterable: AsyncIterable[Any]) -> None:
    async for _ in async_iterable:
        pass


class Stream(AsyncIterator[_T]):
    def __init__(self, src: AsyncIterable[_T] | Iterable[_T]) -> None:
        self._src: AsyncIterator[_T] = ensure_async_iterator(src)
        self._finished: asyncio.Future[_T | _NoValueT] = asyncio.get_event_loop().create_future()
        self._prev_element: _T | _NoValueT = Sentinel.NoValue

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    async def __anext__(self) -> _T:
        try:
            element = await anext(self._src)
            self._prev_element = element
            return element
        except GeneratorExit:
            self._finished.set_result(self._prev_element)
            raise

    def __await__(self) -> Generator[Any, None, _T]:
        result = yield from self.run().__await__()
        return result

    async def run(self) -> _T:
        async for _ in self:
            pass
        if self._prev_element is Sentinel.NoValue:
            raise ValueError("Stream is empty")
        return self._prev_element

    async def reduce(
        self,
        reducer: SyncFn[[_T, _T], _T],  # todo - accept async reducer
        initial: _T | _NoValueT = Sentinel.NoValue,
    ) -> _T:
        if initial is Sentinel.NoValue:
            initial = await anext(self._src)

        total = initial
        async for element in self:
            total = reducer(total, element)
        return total

    @overload
    def transform(self, transform: Transformer[_T, _R]) -> Stream[_R]:
        ...

    @overload
    def transform(self, transform: CoroFn[[_T], _R]) -> Stream[_R]:
        ...

    @overload
    def transform(self, transform: SyncFn[[_T], _R]) -> Stream[_R]:
        ...

    def transform(
        self, transform: Transformer[_T, _R] | CoroFn[[_T], _R] | SyncFn[[_T], _R]
    ) -> Stream[_R]:

        t: Transformer[_T, _R]

        # Stream / Transformer -> Stream @ Transformer -> Stream
        if isinstance(transform, Transformer):
            t = transform

        # Stream / Callable -> Map(Callable) @ Stream -> Stream
        elif callable(transform):
            if getattr(transform, "_is_transformer_fn", False):
                # Allow no-argument transformers to be used as a class.
                #
                # For instance:
                #
                #     @transformer
                #     async def double(async_iter: AsyncIterator[float]) -> AsyncIterator[float]:
                #         async for n in async_iter:
                #             yield n * 2
                #
                #     ...
                #
                #     async for nn in arange(100) / double:
                #         # This works
                #         print(nn)

                # todo make this typing-compatible (e.g. namedtuple) and if possible statically
                #  infer whether this callable can be used like this (it can't if the wrapped
                #  function has required args other than the async_iter)
                t = transform()

            else:
                t = Map(transform)

        else:
            raise TypeError(f"Expected a Transformer or Callable, got {type(transform)}")

        return Stream(t.transform(self))

    __truediv__ = transform

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

    @overload
    def __pos__(self: Stream[AsyncIterable[_U]]) -> Stream[_U]:
        ...

    @overload
    def __pos__(self: Stream[Iterable[_U]]) -> Stream[_U]:
        ...

    def __pos__(self: Stream[AsyncIterable[_U]] | Stream[Iterable[_U]]) -> Stream[_U]:
        """Flatten the stream."""
        return Stream(aflatten(self))


class Map(Transformer[_I, _O]):

    __slots__ = ("_fn",)

    @overload
    def __init__(self, fn: CoroFn[[_I], _O]) -> None:
        ...

    @overload
    def __init__(self, fn: SyncFn[[_I], _O]) -> None:
        ...

    def __init__(self, fn: CoroFn[[_I], _O] | SyncFn[[_I], _O]) -> None:
        self._fn = cast(Callable[[_I], Coroutine[Any, Any, _O]], ensure_coroutine_function(fn))

    async def transform(self, src: AsyncIterator[_I]) -> AsyncIterator[_O]:
        async for item in src:
            yield await self._fn(item)


class FlatMap(Transformer[_I, _O]):

    __slots__ = ("_fn",)

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

    async def transform(self, src: AsyncIterator[_I]) -> AsyncIterator[_O]:
        async for item in src:
            async for sub_item in ensure_async_iterator(await self._fn(item)):
                yield sub_item


class Filter(Transformer[_T, _T]):

    __slots__ = ("_fn",)

    @overload
    def __init__(self, fn: CoroFn[[_T], bool]) -> None:
        ...

    @overload
    def __init__(self, fn: SyncFn[[_T], bool]) -> None:
        ...

    def __init__(self, fn: CoroFn[[_T], bool] | SyncFn[[_T], bool]) -> None:
        self._fn = ensure_coroutine_function(fn)

    async def transform(self, src: AsyncIterator[_T]) -> AsyncIterator[_T]:
        async for item in src:
            if await self._fn(item):
                yield item


class TransformerPipeline(Transformer[_I, _O]):

    __slots__ = ("t_a", "t_b")

    def __init__(self, t_a: Transformer[_I, _U], t_b: Transformer[_U, _O]) -> None:
        self._t_a = t_a
        self._t_b = t_b

    async def transform(self, src: AsyncIterator[_I]) -> AsyncIterator[_O]:
        t1 = self._t_a.transform(src)
        t2 = self._t_b.transform(t1)
        async for item in t2:
            yield item


class FnTransformer(Transformer[_I, _O]):

    __slots__ = ("_fn",)

    def __init__(self, fn: Callable[[AsyncIterator[_I]], AsyncIterator[_O]]) -> None:
        self._fn = fn

    async def transform(self, src: AsyncIterator[_I]) -> AsyncIterator[_O]:
        # todo - accept EitherIterable for any transform?
        _async_iterator_src = ensure_async_iterator(src)
        async for item in self._fn(_async_iterator_src):
            yield item


class Sink(FnTransformer[_I, _O]):
    ...


class _SinkFnWrapper(Generic[_P, _A, _B]):

    __slots__ = ("_fn",)

    def __init__(
        self, _fn: Callable[Concatenate[AsyncIterator[_A], _P], Coroutine[object, object, _B]]
    ) -> None:
        self._fn = _fn

    def __call__(self, *__args: _P.args, **__kwargs: _P.kwargs) -> Sink[_A, _B]:
        @wraps(self._fn)
        async def _inner(src: AsyncIterator[_A]) -> AsyncIterator[_B]:
            yield await self._fn(src, *__args, **__kwargs)

        return Sink(_inner)


sink = _SinkFnWrapper


def transformer(
    _fn: Callable[Concatenate[AsyncIterator[_A], _P], AsyncIterator[_B]]
) -> Callable[_P, FnTransformer[_A, _B]]:
    def _outer(*__args: _P.args, **__kwargs: _P.kwargs) -> FnTransformer[_A, _B]:
        def _inner(_src: AsyncIterator[_A]) -> AsyncIterator[_B]:
            return _fn(_src, *__args, **__kwargs)

        return FnTransformer(_inner)

    setattr(_outer, "_is_transformer_fn", True)
    return wraps(_fn)(_outer)


@overload
def stream(__fn: Callable[_P, Iterable[_T]]) -> Callable[_P, Stream[_T]]:
    ...


@overload
def stream(__fn: Callable[_P, AsyncIterable[_T]]) -> Callable[_P, Stream[_T]]:
    ...


def stream(
    __fn: Callable[_P, AsyncIterable[_T]] | Callable[_P, Iterable[_T]]
) -> Callable[_P, Stream[_T]]:
    @wraps(__fn)
    def _outer(*__args: _P.args, **__kwargs: _P.kwargs) -> Stream[_T]:
        return Stream(__fn(*__args, **__kwargs))

    return _outer


__all__ = (
    "Filter",
    "FlatMap",
    "FnTransformer",
    "Map",
    "sink",
    "Sink",
    "stream",
    "Stream",
    "Transformer",
    "transformer",
    "TransformerPipeline",
)
