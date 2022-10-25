from __future__ import annotations

import asyncio
import string
from collections.abc import (
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    AsyncGenerator,
    Awaitable,
)
from functools import wraps
from inspect import isasyncgen
from typing import (
    TypeVar,
    Any,
    TypeAlias,
    Union,
    Generic,
    Protocol,
    cast,
    overload,
    ParamSpec,
    runtime_checkable,
    Type,
    Iterator,
    Iterable,
)

from asyncutils.afilter import afilter
from asyncutils.amap import amap
from asyncutils.amerge import amerge

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")

P = ParamSpec("P")
TypeT = TypeVar("TypeT", bound=type)
_T_co = TypeVar("_T_co", covariant=True)


class ExtendedAsyncIterator(Generic[T], AsyncIterator[T]):
    def __init__(
        self,
        async_iterable: AsyncIterable[T],
        exc_handler: Callable[[tuple[T, BaseException]], Any]
        | Callable[[tuple[T, BaseException]], Coroutine[Any, Any, Any]]
        | None = None,
    ) -> None:
        self._original_async_iterator = aiter(async_iterable)
        self._exc_handler = exc_handler
        self._aiter = self._build_aiter()

    async def _build_aiter(self) -> AsyncIterator[T]:
        # todo - is there a way to continue iteration after an exception?
        while True:
            try:
                yield await anext(self._original_async_iterator)
            except StopAsyncIteration:
                break

    async def __anext__(self) -> T:
        return await anext(self._aiter)

    @overload
    def __amap__(self, fn: Callable[[T], Coroutine[Any, Any, U]]) -> ExtendedAsyncIterator[U]:
        ...

    @overload
    def __amap__(self, fn: Callable[[T], U]) -> ExtendedAsyncIterator[U]:
        ...

    def __amap__(self, fn: Callable[[T], Any]) -> ExtendedAsyncIterator[Any]:
        return ExtendedAsyncIterator(
            async_iterable=amap(fn, self._original_async_iterator, exc_handler=self._exc_handler),
            exc_handler=self._exc_handler,
        )

    amap = __amap__
    __truediv__ = __amap__

    def _set_aiter_exc_handler(
        self,
        fn: Callable[[tuple[T, BaseException]], Any]
        | Callable[[tuple[T, BaseException]], Coroutine[Any, Any, Any]]
        | None,
    ) -> ExtendedAsyncIterator[T]:
        self._exc_handler = fn
        return self

    set_exc_handler = _set_aiter_exc_handler
    # __pow__ = _set_aiter_exc_handler
    __mul__ = _set_aiter_exc_handler

    def __afilter__(self, fn: Callable[[T], bool]) -> ExtendedAsyncIterator[T]:
        return ExtendedAsyncIterator(
            async_iterable=afilter(
                fn, self._original_async_iterator, exc_handler=self._exc_handler
            ),
            exc_handler=self._exc_handler,
        )

    afilter = __afilter__
    __mod__ = __afilter__

    @overload
    async def __gather__(self, fn: Callable[[AsyncIterable[T]], Coroutine[Any, Any, U]]) -> U:
        ...

    @overload
    async def __gather__(self, fn: Callable[[Iterable[T]], U]) -> U:
        ...

    async def __gather__(
        self,
        fn: Callable[[Iterable[T]], U] | Callable[[AsyncIterable[T]], Coroutine[Any, Any, U]],
    ) -> U:
        if asyncio.iscoroutinefunction(fn):
            _fn = cast(Callable[[AsyncIterable[T]], Coroutine[Any, Any, U]], fn)
            return await _fn(self)
        else:
            _fn_sync = cast(Callable[[Iterable[T]], U], fn)
            return _fn_sync([x async for x in self])

    gather = __gather__
    __matmul__ = __gather__

    def __amerge__(self, *others: AsyncIterable[T]) -> ExtendedAsyncIterator[T]:
        if isinstance(self._original_async_iterator, amerge):
            for other in others:
                self._original_async_iterator.add_async_iter(other)
        else:
            self._original_async_iterator = amerge(self._original_async_iterator, *others)
        return self

    amerge = __amerge__
    __rshift__ = __amerge__
    __rrshift__ = __amerge__
    __lshift__ = __amerge__
    __rlshift__ = __amerge__


@runtime_checkable
class AsyncIterableT(Protocol[T]):
    __aiter__: Callable[[], AsyncIterator[T]]


@overload
def eas(fn: AsyncIterator[T]) -> ExtendedAsyncIterator[T]:
    ...


@overload
def eas(fn: AsyncIterableT[T]) -> AsyncIterableT[T]:
    ...


@overload
def eas(fn: Callable[P, AsyncIterator[T]]) -> Callable[P, ExtendedAsyncIterator[T]]:
    ...


def eas(
    fn: Callable[P, AsyncIterator[T]] | AsyncIterableT[T] | AsyncIterator[T]
) -> Callable[P, ExtendedAsyncIterator[T]] | AsyncIterableT[T] | ExtendedAsyncIterator[T]:
    """A decorator that allows you to use the extended async iterator syntax.

    Can be used as a decorator to an async generator function, as a decorator to a class with an
    __aiter__ method, or as function which transforms an async iterator into an extended async
    iterator.

    todo - fix typing for eas class decorator. Classes decorated aren't currently typed correctly.

    """

    if isinstance(fn, AsyncIterator):
        # Active async iterator
        return ExtendedAsyncIterator(fn)
    elif isinstance(fn, AsyncIterableT):
        # Class with __aiter__ method
        cs = fn
        cs.__aiter__ = eas(cs.__aiter__)
        return cs
    else:
        # Function returning async iterator
        _fn = fn

        @wraps(_fn)
        def _inner(*args: P.args, **kwargs: P.kwargs) -> ExtendedAsyncIterator[T]:
            return ExtendedAsyncIterator(_fn(*args, **kwargs))

        return _inner


@eas
async def myarrange(stop: int) -> AsyncIterator[int]:
    for i in range(stop):
        yield i


async def main() -> None:
    from asyncutils.misc import arange

    async def double(x: float) -> float:
        return x * 2

    def triple(x: float) -> str:
        return str(x * 3)

    def div(x: float) -> float:
        return 5 / (x - 30)

    def first_decimal_is_even(n: float) -> bool:
        return int(str(n).split(".")[1][0]) % 2 == 0

    async def gather_list(zzz: AsyncIterable[U]) -> list[U]:
        return [x async for x in zzz]

    # async for nxt in (myarrange(10) / double / triple / int / div) ** print:
    ait = (
        myarrange(20)
        * print
        / double
        / triple
        / int
        / div
        % first_decimal_is_even
        / double
        / double
        @ gather_list
    )
    print(await ait)

    ait2 = myarrange(20) / double / triple / int / div % first_decimal_is_even / double / double
    # async for a in ait2:
    #     print(a)

    # reveal_type(nxt)
    async for it in eas(arange(3)) >> arange(5):
        print(it)
    g = arange(5) >> myarrange(5)

    h = arange(5) << myarrange(5)
    k = myarrange(5) << arange(10)

    n = await (k @ gather_list)
    reveal_type(n)

    for aitsss in (g, h):
        async for nxts in aitsss:
            print(aitsss, nxts)
            # todo - keep a name of chained methods for extended async iterator
            #  for meaningful repr print
        # reveal_type(nxts)


if __name__ == "__main__":
    asyncio.run(main())
