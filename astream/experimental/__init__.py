from __future__ import annotations

import asyncio
import inspect
from functools import partial
from operator import itemgetter
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

import math
from mypy.types import TupleType
from typing_extensions import TypeVarTuple, Unpack

from astream.stream_utils import arange_delayed, arange
from stream import transformer

_StreamElementT = TypeVar("_StreamElementT", covariant=True)
_T_co = TypeVar("_T_co", covariant=True)
_T = TypeVar("_T")
_InputT = TypeVar("_InputT", contravariant=True)
_OutputT = TypeVar("_OutputT", covariant=True)


async def flatten(ait: AsyncIterable[Iterable[_T_co]]) -> AsyncIterator[_T_co]:
    async for x in ait:
        for y in x:
            yield y


class Stream(Generic[_StreamElementT], AsyncIterator[_StreamElementT]):
    @overload
    def __init__(self, src_iter: AsyncIterator[_StreamElementT]) -> None:
        ...

    @overload
    def __init__(self, src_iter: AsyncIterable[_StreamElementT]) -> None:
        ...

    def __init__(self, src_iter: AsyncIterable[_StreamElementT]) -> None:
        self._src = aiter(src_iter)

    def flatten(self: Stream[Iterable[_T_co]]) -> Stream[_T_co]:
        return Transformer(flatten).transform(self)

    def __pos__(self: Stream[Iterable[_T_co]]) -> Stream[_T_co]:
        return self.flatten()

    @overload
    def __truediv__(self, other: Transformer[_StreamElementT, _T_co]) -> Stream[_T_co]:
        ...

    @overload
    def __truediv__(
        self, other: Callable[[AsyncIterable[_StreamElementT]], AsyncIterator[_T_co]]
    ) -> Stream[_T_co]:
        ...

    @overload
    def __truediv__(
        self, other: Callable[[_StreamElementT], Coroutine[Any, Any, _T_co]]
    ) -> Stream[_T_co]:
        ...

    @overload
    def __truediv__(self, other: Callable[[_StreamElementT], _T_co]) -> Stream[_T_co]:
        ...

    def __truediv__(
        self,
        other: Transformer[_StreamElementT, _T_co]
        | Callable[[AsyncIterable[_StreamElementT]], AsyncIterator[_T_co]]
        | Callable[[_StreamElementT], Coroutine[Any, Any, _T_co]]
        | Callable[[_StreamElementT], _T_co],
    ) -> Stream[_T_co]:
        if isinstance(other, Transformer):
            return other.transform(self)
        elif inspect.isasyncgenfunction(other):
            return Transformer(other).transform(self)
        else:
            transf = cast(Transformer[_StreamElementT, _T_co], Transformer.map(other))
            return transf.transform(self)

    @overload
    def __mod__(
        self, other: Callable[[_StreamElementT], Coroutine[object, object, bool]]
    ) -> Stream[_StreamElementT]:
        ...

    @overload
    def __mod__(self, other: Callable[[_StreamElementT], bool]) -> Stream[_StreamElementT]:
        ...

    def __mod__(
        self,
        other: Callable[[_StreamElementT], Coroutine[object, object, bool]]
        | Callable[[_StreamElementT], bool],
    ) -> Stream[_StreamElementT]:
        return Transformer.filter(other).transform(self)

    # @overload
    # def __floordiv__(
    #     self: Stream[Iterable[_T_co]],
    #     other: Callable[[_T_co], Coroutine[Any, Any, _OutputT]]
    #     ) -> Stream[_OutputT]:
    #     ...
    #
    # @overload
    # def __floordiv__(self: Stream[Iterable[_T_co]], other: Callable[[_T_co], _OutputT]) -> Stream[_OutputT]:
    #     ...
    #
    # def __floordiv__(
    #     self: Stream[Iterable[_T_co]],
    #     other: Callable[[_T_co], Coroutine[Any, Any, _OutputT] | Callable[[_T_co], Callable[[_T_co], _OutputT]]]
    #     ) -> Stream[_OutputT]:
    #     return Transformer.flatmap(other).transform(self)

    async def __anext__(self) -> _StreamElementT:
        while True:
            try:
                return await self._src.__anext__()
            except StopAsyncIteration:
                raise

    async def to_list(self) -> list[_StreamElementT]:
        return [x async for x in self]


class Transformer(Generic[_InputT, _OutputT]):
    def __init__(
        self, transformer: Callable[[AsyncIterable[_InputT]], AsyncIterator[_OutputT]]
    ) -> None:
        self._transformer = transformer

    @classmethod
    @overload
    def map(
        cls, f: Callable[[_InputT], Coroutine[Any, Any, _OutputT]]
    ) -> Transformer[_InputT, _OutputT]:
        ...

    @classmethod
    @overload
    def map(cls, f: Callable[[_InputT], _OutputT]) -> Transformer[_InputT, _OutputT]:
        ...

    @classmethod
    def map(
        cls, f: Callable[[_InputT], Coroutine[Any, Any, _OutputT]] | Callable[[_InputT], _OutputT]
    ) -> Transformer[_InputT, _OutputT]:

        if inspect.iscoroutinefunction(f):
            _async_fn = cast(Callable[[_InputT], Coroutine[Any, Any, _OutputT]], f)

            async def _map(
                src_iter: AsyncIterable[_InputT],
            ) -> AsyncIterator[_OutputT]:
                async for i in src_iter:
                    yield await _async_fn(i)

        else:
            _sync_fn = cast(Callable[[_InputT], _OutputT], f)

            async def _map(
                src_iter: AsyncIterable[_InputT],
            ) -> AsyncIterator[_OutputT]:
                async for i in src_iter:
                    yield _sync_fn(i)

        return cls(_map)

    @classmethod
    @overload
    def filter(
        cls, f: Callable[[_StreamElementT], Coroutine[Any, Any, bool]]
    ) -> Transformer[_StreamElementT, _StreamElementT]:
        ...

    @classmethod
    @overload
    def filter(
        cls, f: Callable[[_StreamElementT], bool]
    ) -> Transformer[_StreamElementT, _StreamElementT]:
        ...

    @classmethod
    def filter(
        cls,
        f: Callable[[_StreamElementT], Coroutine[Any, Any, bool]]
        | Callable[[_StreamElementT], bool],
    ) -> Transformer[_StreamElementT, _StreamElementT]:

        if inspect.iscoroutinefunction(f):
            _async_fn = cast(Callable[[_InputT], Coroutine[Any, Any, bool]], f)

            async def _filter(
                src_iter: AsyncIterable[_InputT],
            ) -> AsyncIterator[_OutputT]:
                async for i in src_iter:
                    if await _async_fn(i):
                        yield cast(_OutputT, i)

        else:
            _sync_fn = cast(Callable[[_InputT], bool], f)

            async def _filter(
                src_iter: AsyncIterable[_InputT],
            ) -> AsyncIterator[_OutputT]:
                async for i in src_iter:
                    if _sync_fn(i):
                        yield cast(_OutputT, i)

        return cast(Transformer[_StreamElementT, _StreamElementT], cls(_filter))

    @classmethod
    @overload
    def flatmap(
        cls, f: Callable[[_InputT], Coroutine[Any, Any, Iterable[_OutputT]]]
    ) -> Transformer[_InputT, _OutputT]:
        ...

    @classmethod
    @overload
    def flatmap(cls, f: Callable[[_InputT], Iterable[_OutputT]]) -> Transformer[_InputT, _OutputT]:
        ...

    @classmethod
    def flatmap(
        cls,
        f: Callable[[_InputT], Coroutine[Any, Any, Iterable[_OutputT]]]
        | Callable[[_InputT], Iterable[_OutputT]],
    ) -> Transformer[_InputT, _OutputT]:

        if inspect.iscoroutinefunction(f):
            _async_fn = cast(Callable[[_InputT], Coroutine[Any, Any, Iterable[_OutputT]]], f)

            async def _flatmap(
                src_iter: AsyncIterable[_InputT],
            ) -> AsyncIterator[_OutputT]:
                async for i in src_iter:
                    for o in await _async_fn(i):
                        yield o

        else:
            _sync_fn = cast(Callable[[_InputT], Iterable[_OutputT]], f)

            async def _flatmap(
                src_iter: AsyncIterable[_InputT],
            ) -> AsyncIterator[_OutputT]:
                async for i in src_iter:
                    for o in _sync_fn(i):
                        yield o

        return cls(_flatmap)

    def transform(self, stream: AsyncIterable[_InputT]) -> Stream[_OutputT]:
        return Stream(self._transformer(aiter(stream)))

    def compose_with(
        self, second: Transformer[_OutputT, _StreamElementT]
    ) -> Transformer[_InputT, _StreamElementT]:
        return Transformer(partial(second._transformer, self._transformer))

    @overload
    def __truediv__(
        self, other: Transformer[_OutputT, _StreamElementT]
    ) -> Transformer[_InputT, _StreamElementT]:
        ...

    @overload
    def __truediv__(
        self, other: Callable[[AsyncIterable[_OutputT]], AsyncIterator[_StreamElementT]]
    ) -> Transformer[_InputT, _StreamElementT]:
        ...

    @overload
    def __truediv__(
        self, other: Callable[[_OutputT], Coroutine[Any, Any, _StreamElementT]]
    ) -> Transformer[_InputT, _StreamElementT]:
        ...

    @overload
    def __truediv__(
        self, other: Callable[[_OutputT], _StreamElementT]
    ) -> Transformer[_InputT, _StreamElementT]:
        ...

    def __truediv__(
        self,
        other: Transformer[_OutputT, _StreamElementT]
        | Callable[[AsyncIterable[_OutputT]], AsyncIterator[_StreamElementT]]
        | Callable[[_OutputT], Coroutine[Any, Any, _StreamElementT]]
        | Callable[[_OutputT], _StreamElementT],
    ) -> Transformer[_InputT, _StreamElementT]:
        if isinstance(other, Transformer):
            return self.compose_with(other)

        if inspect.isasyncgenfunction(other):
            _other_async_gen = cast(
                Callable[[AsyncIterable[_OutputT]], AsyncIterator[_StreamElementT]], other
            )
            return self.compose_with(Transformer(_other_async_gen))

        if inspect.iscoroutinefunction(other):
            _other_async_fn = cast(
                Callable[[_OutputT], Coroutine[Any, Any, _StreamElementT]], other
            )
            return self.compose_with(Transformer.map(_other_async_fn))

        if callable(other):
            _other_fn = cast(Callable[[_OutputT], _StreamElementT], other)
            return self.compose_with(Transformer.map(_other_fn))

    @overload
    def __rtruediv__(self, other: AsyncIterable[_InputT]) -> Stream[_OutputT]:
        ...

    @overload
    def __rtruediv__(
        self, other: Callable[[AsyncIterable[_StreamElementT]], AsyncIterator[_OutputT]]
    ) -> Transformer[_StreamElementT, _OutputT]:
        ...

    @overload
    def __rtruediv__(
        self, other: Callable[[_StreamElementT], Coroutine[Any, Any, _InputT]]
    ) -> Transformer[_StreamElementT, _OutputT]:
        ...

    @overload
    def __rtruediv__(
        self, other: Callable[[_StreamElementT], _InputT]
    ) -> Transformer[_StreamElementT, _OutputT]:
        ...

    def __rtruediv__(
        self,
        other: AsyncIterable[_InputT]
        | Callable[[AsyncIterable[_StreamElementT]], AsyncIterator[_OutputT]]
        | Callable[[_StreamElementT], Coroutine[Any, Any, _InputT]]
        | Callable[[_StreamElementT], _InputT],
    ) -> Stream[_OutputT] | Transformer[_StreamElementT, _OutputT]:

        if isinstance(other, AsyncIterable):
            return self.transform(other)

        if inspect.isasyncgenfunction(other):
            _other_async_gen_fn = cast(
                Callable[[AsyncIterable[_StreamElementT]], AsyncIterator[_InputT]], other
            )
            return Transformer(_other_async_gen_fn).compose_with(self)

        if inspect.iscoroutinefunction(other):
            _other_async_fn = cast(Callable[[_StreamElementT], Coroutine[Any, Any, _InputT]], other)
            return Transformer.map(_other_async_fn).compose_with(self)

        if callable(other):
            _other_fn = cast(Callable[[_StreamElementT], _InputT], other)
            return Transformer.map(_other_fn).compose_with(self)

    def __mod__(
        self,
        other: Callable[[_OutputT], bool] | Callable[[_OutputT], Coroutine[object, object, bool]],
    ) -> Transformer[_InputT, _OutputT]:
        return self.compose_with(Transformer.filter(other))


_A = TypeVar("_A")
_A_co = TypeVar("_A_co", covariant=True)
_A_contra = TypeVar("_A_contra", contravariant=True)
_B = TypeVar("_B")
_B_co = TypeVar("_B_co", covariant=True)
_B_contra = TypeVar("_B_contra", contravariant=True)
Ts = TypeVarTuple("Ts")

_TT = TypeVar("_TT", bound=tuple[object, object])


class NoValueT:
    pass


NoValue = NoValueT()


class Pairwise(Generic[_T], AsyncIterator[tuple[_T, _T]]):
    def __init__(self, iterable: AsyncIterable[_T]) -> None:
        self._iterator = aiter(iterable)
        self._previous: _T | NoValueT = NoValue

    async def __anext__(self) -> tuple[_T, _T]:
        if self._previous is NoValue:
            self._previous = await anext(self._iterator)
        nxt = await anext(self._iterator)
        previous = self._previous
        self._previous = nxt
        assert not isinstance(previous, NoValueT)
        return previous, nxt


pairwise = transformer(Pairwise)

if __name__ == "__main__":

    if not TYPE_CHECKING:

        def reveal_type(x: Any) -> None:
            print(f"{x} has type {type(x)}")

    async def _main() -> None:

        ps = Pairwise(arange_delayed(10))
        async for p in ps:
            print("hi", p)
            reveal_type(p)
            #  note: Revealed type is "Tuple[builtins.int, builtins.int]"

        exit()

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

        # async def pairwise(iterable: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T]]:

        async def arange(n: int) -> AsyncIterator[int]:
            for i in range(n):
                yield i
                await asyncio.sleep(0.1)

        _D = TypeVar("_D")
        _D_co = TypeVar("_D_co", covariant=True)
        _D_contra = TypeVar("_D_contra", contravariant=True)

        # async def pairwise(iterable: AsyncIterable[_D]) -> AsyncIterator[tuple[_D, _D]]:
        # async def pairwise(iterable: AsyncIterable[_D]) -> AsyncIterator[tuple[_D, _D]]:
        # async def pairwise(iterable: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T]]:
        #     # async def pairwise(iterable: AsyncIterable[_D]) -> AsyncIterator[tuple[_D, _D]]:
        #     iterator = aiter(iterable)
        #     try:
        #         prev, nxt = await anext(iterator), await anext(iterator)
        #     except StopAsyncIteration:
        #         return
        #     yield prev, nxt
        #     async for nxt in iterator:
        #         yield (prev, nxt)
        #         prev = nxt

        stream = Stream(arange(10)) / Pairwise
        reveal_type(stream)
        stream = Stream(arange(10)) / Pairwise
        reveal_type(stream)
        stream = Stream(arange(10)) / Pairwise
        reveal_type(stream)

        # Works as expected, but type checker complains -
        # error: Unsupported operand types for / ("Stream[int]" and "Callable[[AsyncIterable[_T]], AsyncIterator[Tuple[_T, _T]]]")  [operator]

        reveal_type(stream)
        l = await stream.to_list()
        print(l)
        reveal_type(l)

        async def demo_flatten() -> None:
            stream2 = Stream(arange(10)) / mul_2 / returns_float
            reveal_type(stream2)

            stream3 = Stream(arange(10)) / mul_3 / ar
            reveal_type(stream3)

            async for i in +stream3:
                print(i)
                reveal_type(i)

        async def demo_flatmap() -> None:
            stream3 = Stream(arange(10)) / mul_3 / ar
            reveal_type(stream3)

            async for i in stream3:
                print(i)
                reveal_type(i)

        async def demo_compose() -> None:
            s = (
                Stream(arange_delayed(100, delay=0.06))
                / (lambda x: x / 10)
                / math.sin
                / pairwise
                % (lambda x: x[0] < x[1])
                / itemgetter(1)
            )
            async for i in s:
                print(i)
                reveal_type(i)

        # await demo_compose()
        # await demo_to_list()
        # await demo_flatten()
        await demo_flatmap()

    asyncio.run(_main())
