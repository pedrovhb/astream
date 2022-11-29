from __future__ import annotations

import asyncio
import inspect
import operator
from abc import ABC
from collections import deque
from functools import wraps, partial
from types import NotImplementedType
from typing import (
    TypeVar,
    AsyncIterable,
    AsyncIterator,
    Protocol,
    Coroutine,
    Callable,
    Generic,
    TYPE_CHECKING,
    TypeAlias,
    overload,
    ParamSpec,
    cast,
    Union,
    Iterable,
    Sized,
    Literal,
    Sequence,
)


from .utils import NoValueT, NoValue

_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)


_T = TypeVar("_T")
_U = TypeVar("_U")
_R = TypeVar("_R")
_P = ParamSpec("_P")
_CoroT: TypeAlias = Coroutine[object, object, _T_co]
_R_co = TypeVar("_R_co", covariant=True)


def ensure_async_iterator(src: Iterable[_T] | AsyncIterable[_T]) -> AsyncIterator[_T]:
    """Given a sync or async iterable, return an async iterator.

    Args:
        src: The iterable to ensure is async.

    Returns:
        An async iterator that yields the items from the original iterable.
    """
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


def ensure_coroutine_function(
    fn: Callable[_P, _T_co] | Callable[_P, _CoroT[_T_co]]
) -> Callable[_P, _CoroT[_T_co]]:
    """Given a sync or async function, return an async function.

    Args:
        fn: The function to ensure is async.

    Returns:
        An async function that runs the original function.

    """
    if inspect.iscoroutinefunction(fn):
        return fn
    else:
        _fn_sync: Callable[_P, _T_co] = cast(Callable[_P, _T_co], fn)

        @wraps(_fn_sync)
        async def _fn_async(*args: _P.args, **kwargs: _P.kwargs) -> _T_co:
            return _fn_sync(*args, **kwargs)

        return _fn_async


def ensure_async_iterator_fn(
    fn: Callable[_P, Iterable[_R]]
    | Callable[_P, _CoroT[Iterable[_R]]]
    | Callable[_P, AsyncIterable[_R]]
    | Callable[_P, _CoroT[AsyncIterable[_R]]]
) -> Callable[_P, AsyncIterator[_R]]:
    """Given a sync or async function that returns an iterable or async iterable, return an async
    function that returns an async iterator.

    That's a mouthful. Here's an example:

    >>> async def foo(x: int) -> Iterable[int]:
    ...     # Async function which returns a sync iterable.
    ...     await asyncio.sleep(1)
    ...     return range(x)

    >>> async def bar(x: int) -> AsyncIterable[int]:
    ...     # Async function which returns an async iterable.
    ...     async def _bar() -> AsyncIterator[int]:
    ...         for i in range(x):
    ...             yield i
    ...     await asyncio.sleep(1)
    ...     return _bar()

    >>> def baz(x: int) -> AsyncIterator[int]:
    ...     # Sync function which returns a sync iterable.
    ...     return range(x)

    >>> async def main() -> None:
    ...     results_foo = []
    ...     async for i in ensure_async_iterator_fn(foo)(5):
    ...         results_foo.append(i)
    ...     results_bar = []
    ...     async for i in ensure_async_iterator_fn(bar)(5):
    ...         results_bar.append(i)
    ...     results_baz = []
    ...     async for i in ensure_async_iterator_fn(baz)(5):
    ...         results_baz.append(i)
    ...     assert [0, 1, 2, 3, 4] == results_bar == results_baz == results_foo
    ...     print("Success!")
    >>> asyncio.run(main())
    Success!
    """

    async def _async_gen_fn(*__args: _P.args, **__kwargs: _P.kwargs) -> AsyncIterator[_R]:
        _coro_fn: Callable[
            _P, _CoroT[Union[AsyncIterable[_R], Iterable[_R]]]
        ] = ensure_coroutine_function(fn)
        itr = await _coro_fn(*__args, **__kwargs)
        async for item in ensure_async_iterator(itr):
            yield item

    return _async_gen_fn


class BaseTransformer(Generic[_T_contra, _R_co], ABC):
    def transform(self, async_iterable: AsyncIterable[_T_contra]) -> AsyncIterable[_R_co]:
        ...

    @overload
    def _do_transform(self, other: Stream[_T_contra]) -> Stream[_R_co]:
        ...

    @overload
    def _do_transform(
        self, other: AsyncIterable[_T_contra] | Iterable[_T_contra]
    ) -> AsyncIterable[_R_co]:
        ...

    def _do_transform(
        self, other: AsyncIterable[_T_contra] | Iterable[_T_contra]
    ) -> AsyncIterable[_R_co]:
        if isinstance(other, Stream):
            return Stream(self.transform(other))
        if not isinstance(other, AsyncIterable):
            other = ensure_async_iterator(other)
        return self.transform(other)

    @overload
    def __rmatmul__(self, other: Stream[_T_contra]) -> Stream[_R_co]:
        ...

    @overload
    def __rmatmul__(self, other: AsyncIterable[_T_contra]) -> AsyncIterable[_R_co]:
        ...

    def __rmatmul__(self, other: AsyncIterable[_T_contra]) -> AsyncIterable[_R_co]:
        return self._do_transform(other)


class Transformer(BaseTransformer[_T_contra, _R_co]):
    def __init__(self, fn: Callable[[AsyncIterable[_T_contra]], AsyncIterable[_R_co]]) -> None:
        self._fn = fn

    def transform(self, async_iterable: AsyncIterable[_T_contra]) -> AsyncIterable[_R_co]:
        return self._fn(async_iterable)


class Map(BaseTransformer[_T_contra, _R_co]):
    def __init__(
        self, fn: Callable[[_T_contra], _R_co] | Callable[[_T_contra], _CoroT[_R_co]]
    ) -> None:
        self._fn: Callable[[_T_contra], _CoroT[_R_co]] = ensure_coroutine_function(fn)

    def transform(self, async_iterable: AsyncIterable[_T_contra]) -> AsyncIterable[_R_co]:
        async def _mapped() -> AsyncIterator[_R_co]:
            async for x in async_iterable:
                yield await self._fn(x)

        return _mapped()

    @overload
    def __rtruediv__(self, other: Stream[_T_contra]) -> Stream[_R_co]:
        ...

    @overload
    def __rtruediv__(self, other: AsyncIterable[_T_contra]) -> AsyncIterable[_R_co]:
        ...

    def __rtruediv__(
        self, other: AsyncIterable[_T_contra] | Stream[_T_contra]
    ) -> AsyncIterable[_R_co] | Stream[_R_co]:
        return self._do_transform(other)


class Filter(BaseTransformer[_T, _T]):
    def __init__(self, fn: Callable[[_T], bool] | Callable[[_T], _CoroT[bool]]) -> None:
        self._fn: Callable[[_T], _CoroT[bool]] = ensure_coroutine_function(fn)

    def transform(self, async_iterable: AsyncIterable[_T]) -> AsyncIterable[_T]:
        async def _filtered() -> AsyncIterator[_T]:
            async for x in async_iterable:
                if await self._fn(x):
                    yield x

        return _filtered()

    @overload
    def __rmod__(self, other: Stream[_T]) -> Stream[_T]:
        ...

    @overload
    def __rmod__(self, other: AsyncIterable[_T]) -> AsyncIterable[_T]:
        ...

    def __rmod__(self, other: AsyncIterable[_T] | Stream[_T]) -> AsyncIterable[_T] | Stream[_T]:
        return self._do_transform(other)


class FilterFalse(Filter[_T]):
    def transform(self, async_iterable: AsyncIterable[_T]) -> AsyncIterable[_T]:
        async def _filtered() -> AsyncIterator[_T]:
            async for x in async_iterable:
                if not await self._fn(x):
                    yield x

        return _filtered()


class FlatMap(BaseTransformer[_T_contra, _R_co]):
    def __init__(
        self,
        fn: Callable[[_T_contra], Iterable[_R_co]]
        | Callable[[_T_contra], _CoroT[Iterable[_R_co]]]
        | Callable[[_T_contra], AsyncIterable[_R_co]]
        | Callable[[_T_contra], _CoroT[AsyncIterable[_R_co]]],
    ) -> None:
        self._fn: Callable[[_T_contra], AsyncIterable[_R_co]] = ensure_async_iterator_fn(fn)

    def transform(self, async_iterable: AsyncIterable[_T_contra]) -> AsyncIterable[_R_co]:
        async def _flat_mapped() -> AsyncIterator[_R_co]:
            async for x in async_iterable:
                async for y in self._fn(x):
                    yield y

        return _flat_mapped()

    @overload
    def __rfloordiv__(self, other: Stream[_T_contra]) -> Stream[_R_co]:
        ...

    @overload
    def __rfloordiv__(self, other: AsyncIterable[_T_contra]) -> AsyncIterable[_R_co]:
        ...

    def __rfloordiv__(
        self, other: AsyncIterable[_T_contra] | Stream[_T_contra]
    ) -> AsyncIterable[_R_co] | Stream[_R_co]:
        return self._do_transform(other)


class Stream(AsyncIterable[_T]):
    def __init__(self, src: AsyncIterable[_T] | Iterable[_T]) -> None:
        self._src: AsyncIterator[_T] = ensure_async_iterator(src)

    def __aiter__(self) -> AsyncIterator[_T]:
        return self._src.__aiter__()

    @overload
    def __truediv__(self, other: Callable[[AsyncIterable[_T]], AsyncIterable[_R]]) -> Stream[_R]:
        ...

    @overload
    def __truediv__(self, other: Callable[[_T], _R] | Callable[[_T], _CoroT[_R]]) -> Stream[_R]:
        ...

    @overload
    def __truediv__(self, other: object) -> NotImplementedType:
        ...

    def __truediv__(
        self,
        other: Callable[[_T], _R]
        | Callable[[_T], _CoroT[_R]]
        | Callable[[AsyncIterable[_T]], AsyncIterable[_R]]
        | object,
    ) -> Stream[_R] | NotImplementedType:

        if inspect.isasyncgenfunction(other):
            return self @ Transformer(other)
        elif callable(other) and not isinstance(other, BaseTransformer):
            return self / Map(other)
        else:
            return NotImplemented

    @overload
    def __mod__(self, other: Callable[[_T], bool] | Callable[[_T], _CoroT[bool]]) -> Stream[_T]:
        ...

    @overload
    def __mod__(self, other: object) -> NotImplementedType:
        ...

    def __mod__(
        self, other: Callable[[_T], bool] | Callable[[_T], _CoroT[bool]] | object
    ) -> Stream[_T] | NotImplementedType:
        if callable(other) and not isinstance(other, BaseTransformer):
            other = cast(Callable[[_T], bool] | Callable[[_T], _CoroT[bool]], other)
            return self % Filter(other)
        else:
            return NotImplemented

    @overload
    def __floordiv__(
        self,
        other: Callable[[_T], Iterable[_R]]
        | Callable[[_T], _CoroT[Iterable[_R]]]
        | Callable[[_T], AsyncIterable[_R]]
        | Callable[[_T], _CoroT[AsyncIterable[_R]]],
    ) -> Stream[_R]:
        ...

    @overload
    def __floordiv__(self, other: object) -> NotImplementedType:
        ...

    def __floordiv__(
        self,
        other: Callable[[_T], Iterable[_R]]
        | Callable[[_T], _CoroT[Iterable[_R]]]
        | Callable[[_T], AsyncIterable[_R]]
        | Callable[[_T], _CoroT[AsyncIterable[_R]]]
        | object,
    ) -> Stream[_R]:
        if callable(other) and not isinstance(other, BaseTransformer):
            other = cast(
                Callable[[_T], Iterable[_R]]
                | Callable[[_T], _CoroT[Iterable[_R]]]
                | Callable[[_T], AsyncIterable[_R]]
                | Callable[[_T], _CoroT[AsyncIterable[_R]]],
                other,
            )
            return self // FlatMap(other)
        else:
            return NotImplemented


# async def pairwise(async_iterable: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T]]:
#     ait = aiter(async_iterable)
#     previous = await anext(ait)
#     async for x in async_iterable:
#         yield previous, x
#         previous = x


@overload
def n_wise(
    n: Literal[1], fillvalue: _T | NoValueT = ...
) -> Callable[[AsyncIterable[_T]], AsyncIterator[tuple[_T]]]:
    ...


@overload
def n_wise(
    n: Literal[2], fillvalue: _T | NoValueT = ...
) -> Callable[[AsyncIterable[_T]], AsyncIterator[tuple[_T, _T]]]:
    ...


@overload
def n_wise(
    n: Literal[3], fillvalue: _T | NoValueT = ...
) -> Callable[[AsyncIterable[_T]], AsyncIterator[tuple[_T, _T, _T]]]:
    ...


@overload
def n_wise(
    n: Literal[4], fillvalue: _T | NoValueT = ...
) -> Callable[[AsyncIterable[_T]], AsyncIterator[tuple[_T, _T, _T, _T]]]:
    ...


def n_wise(
    n: int,
    fillvalue: _T | NoValueT = NoValue,
) -> Callable[[AsyncIterable[_T]], AsyncIterator[tuple[_T, ...]]]:
    async def _n_wise(async_iterable: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, ...]]:
        window: deque[_T] = deque(maxlen=n)
        if not isinstance(fillvalue, NoValueT):
            window.extend([fillvalue] * (n - 1))

        async for x in async_iterable:
            window.append(x)
            if len(window) == n:
                yield tuple(window)

        if not isinstance(fillvalue, NoValueT):
            for _ in range(n - 1):
                window.append(fillvalue)
                yield tuple(window)

    return _n_wise


async def pairwise(async_iterable: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T]]:
    async for x, y in n_wise(2)(async_iterable):
        yield x, y


async def triplewise(async_iterable: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T, _T]]:
    async for x, y, z in n_wise(3)(async_iterable):
        yield x, y, z


class SameTypeTransformerFn(Protocol):
    def __call__(self, async_iterable: AsyncIterable[_T]) -> AsyncIterator[_T]:
        ...


def take(n: int) -> SameTypeTransformerFn:
    async def _take(async_iterable: AsyncIterable[_T]) -> AsyncIterator[_T]:
        ait = aiter(async_iterable)
        for _ in range(n):
            yield await anext(ait)

    return _take


def skip(n: int) -> SameTypeTransformerFn:
    async def _skip(async_iterable: AsyncIterable[_T]) -> AsyncIterator[_T]:
        ait = aiter(async_iterable)
        for _ in range(n):
            await anext(ait)
        async for x in async_iterable:
            yield x

    return _skip


def skip_while(
    predicate: Callable[[_T], bool] | Callable[[_T], _CoroT[bool]]
) -> Callable[[AsyncIterable[_T]], AsyncIterator[_T]]:
    async def _take_while(async_iterable: AsyncIterable[_T]) -> AsyncIterator[_T]:
        async_predicate = ensure_coroutine_function(predicate)
        async_iterator = aiter(async_iterable)
        while await async_predicate(value := await anext(async_iterator)):
            pass
        yield value
        async for x in async_iterable:
            yield x

    return _take_while


# async def triplewise(
#     async_iterable: AsyncIterable[_T],
#     *,
#     fillvalue: _T | NoValueT = NoValue,
# ) -> AsyncIterator[tuple[_T, _T, _T]]:
#     async for item in n_wise(async_iterable, 3, fillvalue=fillvalue):
#         yield cast(tuple[_T, _T, _T], item)


class TransformerFnT(Protocol[_T, _R]):
    async def __call__(self, async_iterable: AsyncIterable[_T]) -> AsyncIterator[_R]:
        ...


class _NonPartialTransformerFnT(Protocol[_T, _P, _R]):
    async def __call__(
        self, __async_iterable: AsyncIterable[_T], *__args: object, **__kwargs: object
    ) -> AsyncIterator[_R]:
        ...


def transformer(fn: _NonPartialTransformerFnT[_T, _P, _R]) -> Callable[_P, TransformerFnT[_T, _R]]:
    """Decorator to create a transformer function.

    The decorated function must be a coroutine function that takes an async iterable as its first
    argument and returns an async iterator. The function then becomes a callable that takes all
    the original function's arguments (except for the async iterator, the first argument) and
    returns a callable that takes an async iterable and returns an async iterator.

    Example:

        @transformer
        async def a_nwise(stream: AsyncIterable[_T], n: int) -> AsyncIterator[tuple[_T, ...]]:
            window = deque(maxlen=n)
            stream_aiter = aiter(stream)
            for _ in range(n):
                window.append(await anext(stream_aiter))
            yield tuple(window)
            async for x in stream_aiter:
                window.append(x)
                yield tuple(window)

        async def main():
            async for x in Stream(range(10)) / a_nwise(3):
                print(x)
                # (0, 1, 2)
                # (1, 2, 3)
                # ...
    """

    @wraps(fn)
    def _outer(
        *args: _P.args, **kwargs: _P.kwargs  # type: ignore
    ) -> Callable[[AsyncIterable[_T]], AsyncIterator[_R]]:
        async def _inner(async_iterable: AsyncIterable[_T]) -> AsyncIterator[_R]:
            ait = fn(async_iterable, *args, **kwargs)
            async for x in ait:
                yield x

        return _inner

    return cast(Callable[_P, TransformerFnT[_T, _R]], _outer)


@transformer
async def nwise(stream: AsyncIterable[_T], n: int) -> AsyncIterator[tuple[_T, ...]]:
    window = deque(maxlen=n)
    stream_aiter = aiter(stream)
    for _ in range(n):
        window.append(await anext(stream_aiter))
    yield tuple(window)
    async for x in stream_aiter:
        window.append(x)
        yield tuple(window)


async def pairwise(stream: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T]]:
    ait = aiter(stream)
    prev = await anext(ait)
    async for x in ait:
        yield prev, x
        prev = x


async def triplewise(stream: AsyncIterable[_T]) -> AsyncIterator[tuple[_T, _T, _T]]:
    ait = aiter(stream)
    prev = await anext(ait)
    prev2 = await anext(ait)
    async for x in ait:
        yield prev, prev2, x
        prev = prev2
        prev2 = x


@transformer
async def take_while(
    async_iterable: AsyncIterable[_T],
    predicate: Callable[[_T], bool],
) -> AsyncIterator[_T]:
    async_predicate = ensure_coroutine_function(predicate)

    async for x in async_iterable:
        if await async_predicate(x):
            yield x
        else:
            break


def amap(
    fn: Callable[[_T], _R] | Callable[[_T], _CoroT[_R]]
) -> Callable[[AsyncIterable[_T]], AsyncIterator[_R]]:
    async def _amap(async_iterable: AsyncIterable[_T]) -> AsyncIterator[_R]:
        _async_fn = ensure_coroutine_function(fn)
        async for item in async_iterable:
            yield await _async_fn(item)

    return _amap


if __name__ == "__main__":
    # from typing import Callable, TypeVar
    #
    # _T = TypeVar("_T")
    # _R = TypeVar("_R")
    #
    # def apply(x: Callable[_P, _R], y: _T) -> _R:
    #     return x(y)
    #
    # def to_string(x: int) -> str:
    #     return str(x)
    #
    # result = apply(to_string, 1)
    # reveal_type(result)
    #
    # result_2 = apply(lambda x: str(x), 1)  # :(
    # reveal_type(result_2)

    async def main() -> None:
        async def arange(__n: int) -> AsyncIterator[int]:
            for i in range(__n):
                yield i

        def muller(__x: tuple[int, int]) -> str:
            return str(__x) + "!!"

        async for aa in Stream(arange(10)) / pairwise / muller:
            print(aa)
            if TYPE_CHECKING:
                reveal_type(aa)

        exit()
        # borks = Stream([Cat() for _ in range(10)]) / Map(catify) / Map(animalize)
        # borks = Stream([Cat() for _ in range(10)]) / Map(doggify) / Map(animalize)
        # if TYPE_CHECKING:
        #     reveal_type(borks)
        #     reveal_type(list(map(lambda y: str(y), [1, 2, 3])))
        #     reveal_type(Map(bark))
        #     reveal_type(Stream("abcd") / Map(lambda y: y.upper()))

        def is_even(__x: int) -> bool:
            return __x % 2 == 0

        _SizedT = TypeVar("_SizedT", bound=Sized)

        def len_of_first(__x: tuple[str, _T]) -> tuple[int, _T]:
            return (len(__x[0]), __x[1])

        async for yy in Stream(arange(10)) / pairwise / Map(muller):
            print(yy)

        # async for yy in Stream(arange(10)) / Pairwise() % Filter(is_even) / Map(
        #     muller
        # ) / Pairwise() / Map(len_of_first):
        #     print(yy)
        #     if TYPE_CHECKING:
        #         reveal_type(yy)

        def mul_2(x: int) -> int:
            return x * 2

        async def mul_2_coro(x: int) -> int:
            return x * 2

        @overload
        def olo(x: int) -> tuple[int, int]:
            ...

        @overload
        def olo(x: str) -> tuple[int, str]:
            ...

        def olo(x: _T) -> tuple[int, _T]:
            if isinstance(x, int):
                return (x, x)
            elif isinstance(x, str):
                return int(x), x
            else:
                raise TypeError

        def first_fetch(ab: tuple[_T, _U]) -> _U:
            return ab[1]

        async for zz in (
            Stream(arange(10))
            / mul_2
            % (lambda x: x % 2 == 0)
            // range
            / mul_2_coro
            / str
            / olo
            / first_fetch
            / skip(4)
        ):
            print(zz)
            if TYPE_CHECKING:
                reveal_type(zz)

        def lt_5(x: int) -> bool:
            return x < 5

        def i_am_broken(l: Sequence[int]) -> Sequence[str]:
            return list(map(lambda x: str(x), l))

        class AddProtocol(Protocol[_T, _R]):
            def __add__(self, other: _T) -> _R:
                ...

        class S:
            def __add__(self: _T, other: _U) -> Callable[[_T], _U]:
                return partial(operator.add, other)

        it = S()

        def spell_out(x: int) -> list[str]:
            nums = "zero one two three four five six seven eight nine".split()
            return [nums[int(n)] for n in str(x)]

        async for rr in Stream(arange(100)) / skip(30) % is_even / spell_out / " ".join:
            print(rr)
            if TYPE_CHECKING:
                reveal_type(rr)

    asyncio.run(main())
