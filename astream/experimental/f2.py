from __future__ import annotations

import asyncio
import sys
from abc import ABC
from asyncio import Future, Task
from collections import deque
from functools import wraps, partial, singledispatch, singledispatchmethod
from types import TracebackType, NotImplementedType
from typing import *

from astream import ensure_async_iterator, create_future
from astream.formal import ensure_coroutine_function

_T = TypeVar("_T")
_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)

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


class Transformer(Generic[_I, _O], ABC):
    def transform(self, src: AsyncIterable[_I]) -> AsyncIterator[_O]:
        raise NotImplementedError

    ####################################################################
    # __matmul__ (a.k.a. `@`) overloads
    # Apply Transformer
    # - Async?Iterable  @  Transformer     -> Stream
    # - Transformer     @  Transformer     -> TransformerPipeline (which is a Transformer)

    @overload
    def __matmul__(self, other: Transformer[_O, _R]) -> TransformerPipeline[_I, _R]:
        ...

    @overload
    def __matmul__(self, other: object) -> TransformerPipeline[_I, _R] | NotImplementedType:
        ...

    def __matmul__(
        self, other: Transformer[_O, _R] | object
    ) -> TransformerPipeline[_I, _R] | NotImplementedType:

        # Transformer @ Transformer -> TransformerPipeline
        if isinstance(other, Transformer):
            _other = cast(Transformer[_O, _R], other)
            return TransformerPipeline(self, _other)

        return NotImplemented

    @overload
    def __rmatmul__(self, other: EitherIterable[_I]) -> Stream[_O]:
        ...

    @overload
    def __rmatmul__(self, other: object) -> Stream[_O] | NotImplementedType:
        ...

    def __rmatmul__(self, other: EitherIterable[_I] | object) -> Stream[_O] | NotImplementedType:

        # AsyncIterable @ Transformer -> Stream
        if isinstance(other, (AsyncIterable, Iterable)):
            return Stream(other).transform(self)

        return NotImplemented

    ####################################################################
    # __truediv__ (a.k.a. `/`) overloads
    # Apply Transformer
    # - Transformer     /  Transformer     -> TransformerPipeline
    # - Transformer     /  Callable        -> Transformer @ Mapper(Callable)  -> TransformerPipeline
    # - Callable        /  Transformer     -> Mapper(Callable) @ Transformer  -> TransformerPipeline
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

        # Transformer / Transformer -> TransformerPipeline
        if isinstance(other, Transformer):
            return TransformerPipeline(self, other)

        # Transformer / Callable -> Transformer @ Mapper(Callable) -> TransformerPipeline
        if callable(other):
            return TransformerPipeline(self, Mapper(other))

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

        # Callable / Transformer -> Mapper(Callable) @ Transformer -> TransformerPipeline
        if callable(other):
            return TransformerPipeline(Mapper(other), self)

        return NotImplemented

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
        # Stream / Transformer -> Stream @ Transformer -> Stream
        if isinstance(other, Transformer):
            return self.transform(other)

        # Stream / Callable -> Mapper(Callable) @ Stream -> Stream
        if callable(other):
            return self.transform(Mapper(other))

        return NotImplemented


class Mapper(Transformer[_I, _O]):
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

    @classmethod
    def from_partial(
        cls, __fn: Callable[Concatenate[AsyncIterator[_I], _P], AsyncIterator[_O]]
    ) -> Callable[_P, FnTransformer[_I, _O]]:
        def _outer(*__args: _P.args, **__kwargs: _P.kwargs) -> FnTransformer[_I, _O]:
            def _inner(
                __src: AsyncIterator[_I],
            ) -> AsyncIterator[_O]:
                return __fn(__src, *__args, **__kwargs)

            return cls(_inner)

        return _outer


transformer = FnTransformer.from_partial


async def _nwise(async_iterable: AsyncIterable[_T], n: int) -> AsyncIterator[tuple[_T, ...]]:
    # Separate implementation from nwise() because the @transformer decorator
    # doesn't work well with @overload
    async_iterator = aiter(async_iterable)
    d = deque[_T](maxlen=n)

    reached_n = False
    async for item in async_iterator:
        d.append(item)
        if reached_n or len(d) == n:
            reached_n = True
            yield tuple(d)


@overload
def nwise(n: Literal[2]) -> Transformer[_T, tuple[_T, _T]]:
    ...


@overload
def nwise(n: Literal[3]) -> Transformer[_T, tuple[_T, _T, _T]]:
    ...


def nwise(n: int) -> Transformer[_T, tuple[_T, ...]]:
    """Transform an async iterable into an async iterable of n-tuples.

    Args:
        n: The size of the tuples to create.

    Returns:
        A transformer that transforms an async iterable into an async iterable of n-tuples.

    Examples:
        >>> async def main() -> None:
        ...     async for item in Stream(range(4)).transform(nwise(2)):
        ...         print(item)
        >>> asyncio.run(main())
        (0, 1)
        (1, 2)
        (2, 3)
    """
    return FnTransformer(partial(_nwise, n=n))


@transformer
async def repeat(async_iterator: AsyncIterator[_T], n: int) -> AsyncIterator[_T]:
    """Repeats each item in the stream `n` times.

    Args:
        async_iterator: The async iterable to repeat.
        n: The number of times to repeat each item.

    Examples:
        >>> async def main() -> None:
        ...     async for item in range(3) / repeat(2):
        ...         print(item)
        >>> asyncio.run(main())
        0
        0
        1
        1
        2
        2
    """
    async for item in async_iterator:
        for _ in range(n):
            yield item


@transformer
async def take(async_iterator: AsyncIterator[_T], n: int) -> AsyncIterator[_T]:
    for _ in range(n):
        yield await anext(async_iterator)


@transformer
async def drop(async_iterator: AsyncIterator[_T], n: int) -> AsyncIterator[_T]:
    for _ in range(n):
        await anext(async_iterator)
    async for item in async_iterator:
        yield item


@transformer
async def immediately_unique(
    async_iterator: AsyncIterator[_T], key: Callable[[_T], Any] = lambda x: x
) -> AsyncIterator[_T]:
    """Yields only items that are unique from the previous item.

    Examples:
        >>> async def main() -> None:
        ...     async for item in range(5) / repeat(3) / immediately_unique():
        ...         print(item)
        >>> asyncio.run(main())
        0
        1
        2
        3
        4
        >>> async def main() -> None:
        ...     async for item in range(50) / immediately_unique(int.bit_length):
        ...         print(item)
        >>> asyncio.run(main())
        0
        1
        2
        4
        8
        16
        32
    """
    prev = await anext(async_iterator)
    yield prev
    prev_key = key(prev)
    async for item in async_iterator:
        if (new_key := key(item)) != prev_key:
            yield item
            prev_key = new_key


@transformer
async def unique(
    async_iterator: AsyncIterator[_T], key: Callable[[_T], Any] = lambda x: x
) -> AsyncIterator[_T]:
    """Yields only items that are unique across the stream.

    Examples:
        >>> async def main() -> None:
        ...     async for item in range(5) / repeat(3) / unique():
        ...         print(item)
        >>> asyncio.run(main())
        0
        1
        2
        3
        4
        >>> async def main() -> None:
        ...     async for item in range(50, 103, 6) / unique(lambda x: str(x)[0]):
        ...         print(item)
        >>> asyncio.run(main())
        50
        62
        74
        80
        92
    """
    seen: set[Any] = set()
    prev = await anext(async_iterator)
    yield prev
    seen.add(key(prev))
    async for item in async_iterator:
        if (new_key := key(item)) not in seen:
            yield item
            seen.add(new_key)


async def main() -> None:

    sss = Stream(range(10))

    async for item in Stream(range(50)) / repeat(2):
        print(item)
        reveal_type(item)

    reveal_type(nwise(2))

    # async_gen_firstiter, async_gen_finalizer = sys.get_asyncgen_hooks()
    #
    # def async_gen_firstiter_wrapper(agen: AsyncGenerator[Any, Any]) -> None:
    #     print("async_gen_firstiter_wrapper", agen)
    #     async_gen_firstiter(agen)
    #
    # def async_gen_finalizer_wrapper(agen: AsyncGenerator[Any, Any]) -> None:
    #     print("async_gen_finalizer_wrapper", agen)
    #     async_gen_finalizer(agen)
    #
    # sys.set_asyncgen_hooks(async_gen_firstiter_wrapper, async_gen_finalizer_wrapper)

    # s = TransformedStream(
    #     Stream(range(10)),
    #     TransformerPipeline(Mapper(lambda x: x * 2), Filter(lambda x: x % 3 == 0)),
    # )
    def mul_2(x: int) -> float:
        return x / 2

    async def is_halfsies(x: str) -> bool:
        return x.endswith(".5")

    def inv_trim(x: str) -> str:
        return x.split(".")[-1]

    s2 = Stream(range(10)) @ Mapper(mul_2) @ Mapper(str) @ Filter(is_halfsies)
    s3 = Mapper(mul_2) @ nwise(3) @ nwise(2)
    reveal_type(s2)

    async for it in (Stream(range(10)) / s3):
        reveal_type(it)
        a = it[1][0]
        reveal_type(a)  # float - correct

        a = it[2][2]  # error: Tuple index out of range

    # todo - implement l operators for Stream
    tmap = Mapper(mul_2) @ Mapper(str) / float / str % is_halfsies
    reveal_type(tmap)


def ab(x: int) -> str:
    return float(x)


if __name__ == "__main__":

    asyncio.run(main())


#
# class Pipeline(Transformer[_IP, _OP]):
#     def __init__(self, src: AsyncIterable[_IP]) -> None:
#         self._src = src
#         self._graph: dict[Asy, Transformer[_O, _R]] = {}
#
#     def add_transformer(self, transformer: Transformer[_OP, _O]) -> Pipeline[_IP, _O]:
#         self._src = transformer.transform(self._src)
#         return self
