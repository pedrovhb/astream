from __future__ import annotations

import asyncio
import inspect
from functools import partial
from typing import (
    TypeVar,
    Generic,
    Callable,
    Iterable,
    Coroutine,
    Any,
    overload,
    AsyncIterable,
    AsyncIterator,
    cast,
    TYPE_CHECKING,
    TypeAlias,
)

_T = TypeVar("_T", covariant=True)
_U = TypeVar("_U", covariant=True)
_I = TypeVar("_I", contravariant=True)
_O = TypeVar("_O", covariant=True)


if TYPE_CHECKING:
    from typing import *

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

_IterableT = TypeVar("_IterableT", bound=Iterable[object], covariant=True)

FlattenFnT: TypeAlias = Callable[["Stream[Iterable[_U]]"], "Stream[_U]"]


async def flatten(ait: AsyncIterable[Iterable[_U]]) -> AsyncIterator[_U]:
    async for x in ait:
        for y in x:
            yield y


class Stream(Generic[_T], AsyncIterator[_T]):
    def __init__(self, src_iter: AsyncIterable[_T]) -> None:
        self._src = aiter(src_iter)

    def flatten(self: Stream[Iterable[_U]]) -> Stream[_U]:
        return Stream(flatten(self))

    def __pos__(self: Stream[Iterable[_U]]) -> Stream[_U]:
        return self.flatten()

    @overload
    def __truediv__(self, other: Transformer[_T, _O]) -> Stream[_O]:
        ...

    @overload
    def __truediv__(self, other: Callable[[AsyncIterable[_T]], AsyncIterator[_O]]) -> Stream[_O]:
        ...

    @overload
    def __truediv__(self, other: Callable[[_T], Coroutine[Any, Any, _O]]) -> Stream[_O]:
        ...

    @overload
    def __truediv__(self, other: Callable[[_T], _O]) -> Stream[_O]:
        ...

    def __truediv__(
        self,
        other: Transformer[_T, _O]
        | Callable[[AsyncIterable[_T]], AsyncIterator[_O]]
        | Callable[[_T], Coroutine[Any, Any, _O]]
        | Callable[[_T], _O],
    ) -> Stream[_O]:
        if isinstance(other, Transformer):
            return other.transform(self)
        elif inspect.isasyncgenfunction(other):
            return Transformer(other).transform(self)
        else:
            transf = cast(Transformer[_T, _O], Transformer.map(other))
            return transf.transform(self)

    async def __anext__(self) -> _T:
        while True:
            try:
                return await self._src.__anext__()
            except StopAsyncIteration:
                raise


class Transformer(Generic[_I, _O]):
    def __init__(self, transformer: Callable[[AsyncIterable[_I]], AsyncIterable[_O]]) -> None:
        self._transformer = transformer

    @classmethod
    @overload
    def map(cls, f: Callable[[_I], Coroutine[Any, Any, _O]]) -> Transformer[_I, _O]:
        ...

    @classmethod
    @overload
    def map(cls, f: Callable[[_I], _O]) -> Transformer[_I, _O]:
        ...

    @classmethod
    def map(
        cls, f: Callable[[_I], Coroutine[Any, Any, _O]] | Callable[[_I], _O]
    ) -> Transformer[_I, _O]:

        if inspect.iscoroutinefunction(f):
            _async_fn = cast(Callable[[_I], Coroutine[Any, Any, _O]], f)

            async def _map(
                src_iter: AsyncIterable[_I],
            ) -> AsyncIterable[_O]:
                async for i in src_iter:
                    yield await _async_fn(i)

        else:
            _sync_fn = cast(Callable[[_I], _O], f)

            async def _map(
                src_iter: AsyncIterable[_I],
            ) -> AsyncIterable[_O]:
                async for i in src_iter:
                    yield _sync_fn(i)

        return cls(_map)

    @classmethod
    @overload
    def filter(cls, f: Callable[[_T], Coroutine[Any, Any, bool]]) -> Transformer[_T, _T]:
        ...

    @classmethod
    @overload
    def filter(cls, f: Callable[[_T], bool]) -> Transformer[_T, _T]:
        ...

    @classmethod
    def filter(
        cls, f: Callable[[_T], Coroutine[Any, Any, bool]] | Callable[[_T], bool]
    ) -> Transformer[_T, _T]:

        if inspect.iscoroutinefunction(f):
            _async_fn = cast(Callable[[_I], Coroutine[Any, Any, bool]], f)

            async def _filter(
                src_iter: AsyncIterable[_I],
            ) -> AsyncIterable[_O]:
                async for i in src_iter:
                    if await _async_fn(i):
                        yield cast(_O, i)

        else:
            _sync_fn = cast(Callable[[_I], bool], f)

            async def _filter(
                src_iter: AsyncIterable[_I],
            ) -> AsyncIterable[_O]:
                async for i in src_iter:
                    if _sync_fn(i):
                        yield cast(_O, i)

        return cast(Transformer[_T, _T], cls(_filter))

    def transform(self, stream: AsyncIterable[_I]) -> Stream[_O]:
        return Stream(self._transformer(aiter(stream)))

    def compose_with(self, second: Transformer[_O, _T]) -> Transformer[_I, _T]:
        return Transformer(partial(second._transformer, self._transformer))

    @overload
    def __truediv__(self, other: Transformer[_O, _T]) -> Transformer[_I, _T]:
        ...

    @overload
    def __truediv__(
        self, other: Callable[[AsyncIterable[_O]], AsyncIterator[_T]]
    ) -> Transformer[_I, _T]:
        ...

    @overload
    def __truediv__(self, other: Callable[[_O], Coroutine[Any, Any, _T]]) -> Transformer[_I, _T]:
        ...

    @overload
    def __truediv__(self, other: Callable[[_O], _T]) -> Transformer[_I, _T]:
        ...

    def __truediv__(
        self,
        other: Transformer[_O, _T]
        | Callable[[AsyncIterable[_O]], AsyncIterator[_T]]
        | Callable[[_O], Coroutine[Any, Any, _T]]
        | Callable[[_O], _T],
    ) -> Transformer[_I, _T]:
        if isinstance(other, Transformer):
            return self.compose_with(other)

        if inspect.isasyncgenfunction(other):
            _other_async_gen = cast(Callable[[AsyncIterable[_O]], AsyncIterator[_T]], other)
            return self.compose_with(Transformer(_other_async_gen))

        if inspect.iscoroutinefunction(other):
            _other_async_fn = cast(Callable[[_O], Coroutine[Any, Any, _T]], other)
            return self.compose_with(Transformer.map(_other_async_fn))

        if callable(other):
            _other_fn = cast(Callable[[_O], _T], other)
            return self.compose_with(Transformer.map(_other_fn))

    @overload
    def __rtruediv__(self, other: AsyncIterable[_I]) -> Stream[_O]:
        ...

    @overload
    def __rtruediv__(
        self, other: Callable[[AsyncIterable[_T]], AsyncIterator[_O]]
    ) -> Transformer[_T, _O]:
        ...

    @overload
    def __rtruediv__(self, other: Callable[[_T], Coroutine[Any, Any, _I]]) -> Transformer[_T, _O]:
        ...

    @overload
    def __rtruediv__(self, other: Callable[[_T], _I]) -> Transformer[_T, _O]:
        ...

    def __rtruediv__(
        self,
        other: AsyncIterable[_I]
        | Callable[[AsyncIterable[_T]], AsyncIterator[_O]]
        | Callable[[_T], Coroutine[Any, Any, _I]]
        | Callable[[_T], _I],
    ) -> Stream[_O] | Transformer[_T, _O]:

        if isinstance(other, AsyncIterable):
            return self.transform(other)

        if inspect.isasyncgenfunction(other):
            _other_async_gen_fn = cast(Callable[[AsyncIterable[_T]], AsyncIterator[_I]], other)
            return Transformer(_other_async_gen_fn).compose_with(self)

        if inspect.iscoroutinefunction(other):
            _other_async_fn = cast(Callable[[_T], Coroutine[Any, Any, _I]], other)
            return Transformer.map(_other_async_fn).compose_with(self)

        if callable(other):
            _other_fn = cast(Callable[[_T], _I], other)
            return Transformer.map(_other_fn).compose_with(self)


if __name__ == "__main__":

    if not TYPE_CHECKING:

        def reveal_type(x: Any) -> None:
            print(f"{x} has type {type(x)}")

    async def _main() -> None:
        async def arange(n: int) -> AsyncIterator[int]:
            for i in range(n):
                yield i
                await asyncio.sleep(0.1)

        async def mul_2(x: int) -> int:
            return x * 2

        async def mul_3(x: int) -> int:
            return x * 3

        async def ar(x: int) -> tuple[int, ...]:
            return tuple(range(x))

        async def takes_float(x: float) -> int:
            return int(x / 2)

        async def returns_float(x: int) -> float:
            return float(x / 2)

        async def pairwise(iterable: AsyncIterable[_U]) -> AsyncIterator[tuple[_U, _U]]:
            iterator = aiter(iterable)
            try:
                prev, nxt = await anext(iterator), await anext(iterator)
            except StopAsyncIteration:
                return
            yield prev, nxt
            async for nxt in iterator:
                yield prev, nxt
                prev = nxt

        stream = Stream(arange(10)) / mul_2 / takes_float
        reveal_type(stream)

        stream2 = Stream(arange(10)) / mul_2 / returns_float
        reveal_type(stream2)

        stream3 = Stream(arange(10)) / mul_3 / ar
        reveal_type(stream3)

        async for i in +stream3 / pairwise:
            print(i)
            reveal_type(i)

    asyncio.run(_main())
