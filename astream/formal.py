from __future__ import annotations

import abc
import asyncio
import functools
import inspect
import itertools
import random
from abc import ABC
from functools import cached_property
from operator import methodcaller
from types import NotImplementedType, SimpleNamespace
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
    TYPE_CHECKING,
    Collection,
    Final,
)

from loguru import logger

from .utils import NoValueT, NoValue
from .stream_utils import (
    aconcatenate,
    afilter,
    aflatmap,
    amap,
    amerge,
    arepeat,
    atee,
)

_R = TypeVar("_R")


_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)
_U = TypeVar("_U")
_V = TypeVar("_V")
_CoroT: TypeAlias = Coroutine[object, object, _T]


TransformerFunction = Callable[[AsyncIterable[_T]], AsyncIterable[_U]]

_TransformerT = TypeVar("_TransformerT", bound="Transformer[object, object]")


async def _pairwise(src: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T]]:
    prev: _T | NoValueT = NoValue
    async for item in src:
        if not isinstance(prev, NoValueT):
            yield prev, item
        prev = item


def ensure_async_iterator(src: Iterable[_T] | AsyncIterable[_T]) -> AsyncIterator[_T]:
    if isinstance(src, AsyncIterable):
        _async_src = src
        return aiter(_async_src)

    elif isinstance(src, Iterable):
        _sync_src: Iterable[_T] = src

        async def _aiter() -> AsyncIterator[_T]:
            for item in _sync_src:
                yield item

        return _aiter()

    else:
        raise TypeError(f"Invalid source type: {type(src)}")


P = ParamSpec("P")


@overload
def ensure_coroutine_function(fn: Callable[P, _CoroT[_U]]) -> Callable[P, _CoroT[_U]]:
    ...


@overload
def ensure_coroutine_function(fn: Callable[P, _U]) -> Callable[P, _CoroT[_U]]:
    ...


def ensure_coroutine_function(
    fn: Callable[P, _CoroT[_U]] | Callable[P, _U]
) -> Callable[P, _CoroT[_U]]:
    if inspect.iscoroutinefunction(fn):
        return fn
    else:
        _fn_sync: Callable[P, _U] = cast(Callable[P, _U], fn)

        @functools.wraps(_fn_sync)
        async def _fn_async(*args: P.args, **kwargs: P.kwargs) -> _U:
            return _fn_sync(*args, **kwargs)

        return _fn_async


class TransformableAsyncIterable(AsyncIterable[_T], Generic[_T], ABC):
    def __transform__(self, transformer: Transformer[_T, _U]) -> AsyncIterable[_U]:
        ...


class Transformer(Generic[_T, _U], AsyncIterable[_U]):
    def __init__(
        self,
        src: Iterable[_T] | AsyncIterable[_T],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._src = src

    @cached_property
    def _src_async_iterator(self) -> AsyncIterator[_T]:
        return ensure_async_iterator(self._src)

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator[_U]:
        ...

    # @classmethod
    # def with_args(
    #     cls,
    #     *args: Any,
    #     **kwargs: Any,
    # ) -> Callable[[Iterable[_T] | AsyncIterable[_T]], _TransformerT]:
    #     def _make_transformer(src: Iterable[_T] | AsyncIterable[_T]) -> _TransformerT:
    #         return cls(src, *args, **kwargs)
    #
    #     return _make_transformer

    def __rmatmul__(self, other: AsyncIterable[_T]) -> AsyncIterable[_U]:
        if isinstance(other, TransformableAsyncIterable):
            return other.__transform__(self)
        else:
            return type(super())(other)


class ApplyTransform(Transformer[_T, _U]):
    def __init__(
        self,
        src: Iterable[_T] | AsyncIterable[_T],
        fn: Callable[[AsyncIterator[_T]], AsyncIterator[_U]],
    ) -> None:
        super().__init__(src)
        self._fn = fn

    @cached_property
    def _transformed(self) -> AsyncIterator[_U]:
        return aiter(self._fn(self._src_async_iterator))

    async def __aiter__(self) -> AsyncIterator[_U]:
        async for item in self._transformed:
            yield item


class Map(Transformer[_T, _U]):
    @overload
    def __init__(
        self,
        src: AsyncIterable[_T] | Iterable[_T],
        fn: Callable[[_T], _CoroT[_U]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        src: AsyncIterable[_T] | Iterable[_T],
        fn: Callable[[_T], _U],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ...

    def __init__(
        self,
        src: AsyncIterable[_T] | Iterable[_T],
        fn: Callable[[_T], _CoroT[_U]] | Callable[[_T], _U],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(src, *args, **kwargs)
        self._fn = fn

    @cached_property
    def _fn_async(self) -> Callable[[_T], Coroutine[Any, Any, _U]]:
        return ensure_coroutine_function(self._fn)

    async def __aiter__(self) -> AsyncIterator[_U]:
        async for item in self._src_async_iterator:
            yield await self._fn_async(item)


class Filter(Transformer[_T, _T]):
    @overload
    def __init__(
        self,
        src: AsyncIterable[_T] | Iterable[_T],
        fn: Callable[[_T], _CoroT[bool]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        src: AsyncIterable[_T] | Iterable[_T],
        fn: Callable[[_T], bool],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ...

    def __init__(
        self,
        src: AsyncIterable[_T] | Iterable[_T],
        fn: Callable[[_T], _CoroT[bool]] | Callable[[_T], bool],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(src, *args, **kwargs)
        self._fn = fn

    @cached_property
    def _fn_async(self) -> Callable[[_T], Coroutine[Any, Any, bool]]:
        return ensure_coroutine_function(self._fn)

    async def __aiter__(self) -> AsyncIterator[_T]:
        async for item in self._src_async_iterator:
            if await self._fn_async(item):
                yield item


class FilterFalse(Filter[_T]):
    async def __aiter__(self) -> AsyncIterator[_T]:
        async for item in self._src_async_iterator:
            if not await self._fn_async(item):
                yield item


class FlatMap(Transformer[_T, _U]):
    @overload
    def __init__(
        self,
        src: Iterable[_T] | AsyncIterable[_T],
        fn: Callable[[_T], _CoroT[Iterable[_U]]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        src: Iterable[_T] | AsyncIterable[_T],
        fn: Callable[[_T], Iterable[_U]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        src: Iterable[_T] | AsyncIterable[_T],
        fn: Callable[[_T], _CoroT[AsyncIterable[_U]]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        src: Iterable[_T] | AsyncIterable[_T],
        fn: Callable[[_T], AsyncIterable[_U]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        ...

    def __init__(
        self,
        src: Iterable[_T] | AsyncIterable[_T],
        fn: Callable[[_T], Iterable[_U]]
        | Callable[[_T], _CoroT[Iterable[_U]]]
        | Callable[[_T], AsyncIterable[_U]]
        | Callable[[_T], _CoroT[AsyncIterable[_U]]],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self._fn = fn
        super().__init__(src, *args, **kwargs)

    @cached_property
    def _fn_async_iter(self) -> Callable[[_T], AsyncIterator[_U]]:

        if inspect.isasyncgenfunction(self._fn):
            # Function is async generator function, e.g.
            #    async def _fn(item: _T) -> AsyncIterator[_U]:
            #        yield ...

            _fn_async_gen = cast(Callable[[_T], AsyncIterable[_U]], self._fn)

            async def _fn_async_iter(item: _T) -> AsyncIterator[_U]:
                async for sub_item in _fn_async_gen(item):
                    yield sub_item

            return _fn_async_iter

        elif inspect.iscoroutinefunction(self._fn):
            # Function is async function, e.g.
            #    async def _fn(item: _T) -> Iterable[_U]:
            #        return range(10)

            _fn_async = cast(Callable[[_T], _CoroT[Iterable[_U]]], self._fn)

            async def _fn_async_iter(item: _T) -> AsyncIterator[_U]:
                for sub_item in await _fn_async(item):
                    yield sub_item

            return _fn_async_iter

        elif inspect.isgeneratorfunction(self._fn):
            # Function is generator function, e.g.
            #    def _fn(item: _T) -> Iterable[_U]:
            #        yield ...

            _fn_sync = cast(Callable[[_T], Iterable[_U]], self._fn)

            async def _fn_async_iter(item: _T) -> AsyncIterator[_U]:
                for sub_item in _fn_sync(item):
                    yield sub_item

            return _fn_async_iter

        elif inspect.isfunction(self._fn):
            # Function is sync function, e.g.
            #    def _fn(item: _T) -> Iterable[_U]:
            #        return range(10)

            _fn_sync = cast(Callable[[_T], Iterable[_U]], self._fn)

            async def _fn_async_iter(item: _T) -> AsyncIterator[_U]:
                for sub_item in _fn_sync(item):
                    yield sub_item

            return _fn_async_iter

        else:
            raise TypeError(f"Invalid function type for FlatMap: {type(self._fn)}")

    async def __aiter__(self) -> AsyncIterator[_U]:
        # FlatMap takes one input element of any type, applies a function which returns an iterable,
        # and then yields each element of that iterable.

        async for item in self._src_async_iterator:
            async for sub_item in self._fn_async_iter(item):
                yield sub_item


class _TTTransformerT(Protocol[_T, _U]):
    def __call__(self, item: AsyncIterable[_T]) -> Transformer[_T, _U]:
        ...


class Stream(TransformableAsyncIterable[_T], AsyncIterable[_T]):
    def __init__(
        self,
        src: Iterable[_T] | AsyncIterable[_T],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._src = src

        _metadata = metadata or {}
        if isinstance(self._src, Stream):
            _metadata = {**self._src.metadata.__dict__, **_metadata}
        _metadata["stream_graph"] = _metadata.get("stream_graph") or []
        _metadata["stream_graph_2"] = _metadata.get("stream_graph_2") or []
        self._metadata = SimpleNamespace(**_metadata)

    @property
    def metadata(self) -> SimpleNamespace:
        return self._metadata

    @cached_property
    def _src_async_iterator(self) -> AsyncIterator[_T]:
        return ensure_async_iterator(self._src)

    async def __aiter__(self) -> AsyncIterator[_T]:
        async for item in self._src_async_iterator:
            yield item

    def __repr__(self) -> str:
        return f"Stream({self._src!r}, metadata={self.metadata!r})"

    @overload
    def map(self, fn: Callable[[_T], _CoroT[_U]]) -> Stream[_U]:
        ...

    @overload
    def map(self, fn: Callable[[_T], _U]) -> Stream[_U]:
        ...

    def map(self, fn: Callable[[_T], _CoroT[_U]] | Callable[[_T], _U]) -> Stream[_U]:
        new_stream = Stream(Map(self, fn))
        new_stream.metadata.stream_graph.append(("map", fn))
        new_stream.metadata.stream_graph_2.append((self, new_stream))
        return cast(Stream[_U], new_stream)

    @overload
    def flat_map(self, fn: Callable[[_T], Iterable[_U]]) -> Stream[_U]:
        ...

    @overload
    def flat_map(self, fn: Callable[[_T], _CoroT[Iterable[_U]]]) -> Stream[_U]:
        ...

    @overload
    def flat_map(self, fn: Callable[[_T], AsyncIterable[_U]]) -> Stream[_U]:
        ...

    @overload
    def flat_map(self, fn: Callable[[_T], _CoroT[AsyncIterable[_U]]]) -> Stream[_U]:
        ...

    def flat_map(
        self,
        fn: Callable[[_T], Iterable[_U]]
        | Callable[[_T], _CoroT[Iterable[_U]]]
        | Callable[[_T], AsyncIterable[_U]]
        | Callable[[_T], _CoroT[AsyncIterable[_U]]],
    ) -> Stream[_U]:
        new_stream = Stream(FlatMap(self, fn))
        new_stream.metadata.stream_graph.append(("flat_map", fn))
        new_stream.metadata.stream_graph_2.append((self, new_stream))
        return new_stream

    @overload
    def filter(self, fn: Callable[[_T], _CoroT[bool]]) -> Stream[_T]:
        ...

    @overload
    def filter(self, fn: Callable[[_T], bool]) -> Stream[_T]:
        ...

    def filter(self, fn: Callable[[_T], _CoroT[bool]] | Callable[[_T], bool]) -> Stream[_T]:
        new_stream = Stream(Filter(self, fn))
        new_stream.metadata.stream_graph.append(("filter", fn))
        new_stream.metadata.stream_graph_2.append((self, new_stream))
        return new_stream

    def __transform__(self, transformer: _TTTransformerT[_T, _U]) -> Stream[_U]:
        new_stream = Stream(transformer(self))
        new_stream.metadata.stream_graph.append(("transform", transformer))
        new_stream.metadata.stream_graph_2.append((self, new_stream))
        return new_stream

    # @overload
    # def apply_transform(
    #     self,
    #     transformer: Callable[[AsyncIterator[_T]], Transformer[_T, _U]],
    # ) -> Stream[_U]:
    #     ...

    # @overload
    # def apply_transform(
    #     self,
    #     transformer: Type[Transformer[_T, tuple[_T, _T]]],
    # ) -> Stream[tuple[_T, _T]]:
    #     ...  # works fine

    @overload
    def apply_transform(
        self,
        transformer: StreamTransformer[_T, _U],
    ) -> Stream[_U]:
        ...

    @overload
    def apply_transform(
        self,
        transformer: Type[StreamTransformer[_T, _U]],
    ) -> Stream[_U]:
        ...

    def apply_transform(
        self,
        transformer: Type[StreamTransformer[_T, _U]] | StreamTransformer[_T, _U],
    ) -> Stream[_U]:
        new_stream = transformer.__stream_transform__(self)
        new_stream.metadata.stream_graph.append(("apply_transform", transformer))
        new_stream.metadata.stream_graph_2.append((self, new_stream))
        return new_stream

    # def apply_transform(
    #     self,
    #     transformer: StreamTransformer[_T, _U],
    # ) -> Stream[_U]:
    #     new_stream = transformer.__stream_transform__(self)
    #     new_stream.metadata.stream_graph.append(("apply_transform", transformer))
    #     new_stream.metadata.stream_graph_2.append((self, new_stream))
    #     return new_stream

    __truediv__ = map
    __floordiv__ = flat_map
    __mod__ = filter
    # __matmul__ = apply_transform

    def pairwise(self) -> Stream[tuple[_T, _T]]:
        return Stream(_pairwise(self))

    @logger.catch
    def fork(self) -> tuple[Stream[_T], Stream[_T]]:
        a, b = atee(self, 2)
        # self._src_async_iterator = a
        return a, b


# todo invert - StreamTransformer returns new stream, __truediv__ etc calls StreamTransformer
#  and StreamTransformer defines __rmatmul__ etc


def _sync_function(item: int) -> int:
    return item * 2


async def _async_function(item: int) -> int:
    return item * 2


def _sync_iterable() -> Iterable[int]:
    return range(10)


async def _async_iterable() -> AsyncIterable[int]:
    for i in range(10):
        yield i


def _range_of_ranges(num: int) -> Iterable[Iterable[int]]:
    for i in range(num):
        yield range(i)


def _lo_range(num: int) -> Iterable[int]:
    return range(num)


async def _async_range_of_ranges() -> AsyncIterable[Iterable[int]]:
    for i in range(10):
        yield range(i)


async def _summer(over: Iterable[int]) -> int:
    return sum(over)


@runtime_checkable
class StreamTransformer(Protocol[_T, _U]):
    @staticmethod
    def __stream_transform__(stream: Stream[_T]) -> Stream[_U]:
        ...


Enumerate: Callable[[AsyncIterable[_T]], Map[_T, tuple[int, _T]]] = Map[
    _T, tuple[int, _T]
].with_args(enumerate)


class Pairwise(StreamTransformer[_T, tuple[_T, _T]]):
    @staticmethod
    def __stream_transform__(stream: Stream[_T]) -> Stream[tuple[_T, _T]]:
        async def _pairwise(
            src: AsyncIterator[_T],
        ) -> AsyncIterator[tuple[_T, _T]]:
            prev = await src.__anext__()
            async for item in src:
                yield prev, item
                prev = item

        return Stream(_pairwise(aiter(stream)))


def random_enumerate(item: list[_T]) -> tuple[int, list[_T]]:
    return random.randint(0, 100), item


@logger.catch
async def main() -> None:

    # async for i in Map(_sync_iterable(), _sync_function):
    #     print(i)
    #     if TYPE_CHECKING:
    #         reveal_type(i)
    #
    # async for i in Map(_async_iterable(), _async_function):
    #     print(i)
    #     if TYPE_CHECKING:
    #         reveal_type(i)
    #
    # async for i in Map(_sync_iterable(), _async_function):
    #     print(i)
    #     if TYPE_CHECKING:
    #         reveal_type(i)
    #
    # async for i in Map(_async_iterable(), _sync_function):
    #     print(i)
    #     if TYPE_CHECKING:
    #         reveal_type(i)
    #
    # async for ii in FlatMap(_sync_iterable(), _lo_range):
    #     print(ii)
    #     if TYPE_CHECKING:
    #         reveal_type(ii)
    #
    # # async for j in Stream(_async_iterable()) / _sync_function:
    # #     print(j)
    # #     if TYPE_CHECKING:
    # #         reveal_type(j)
    #
    # async for j in Stream(_async_iterable()) % (lambda x: x % 2 == 0) // (lambda x: range(x)):
    #     print(j)
    #     if TYPE_CHECKING:
    #         reveal_type(j)

    async for i in Stream(_async_iterable()) / (lambda x: x * 2):
        print(i)
        if TYPE_CHECKING:
            reveal_type(i)

    # import rich
    #
    # rich.print(s.metadata.stream_graph)
    # rich.print(s.metadata.stream_graph_2)


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
