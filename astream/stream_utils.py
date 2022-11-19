from __future__ import annotations

import asyncio
import math
import random
from abc import abstractmethod
from asyncio import Future
from datetime import timedelta
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Iterable,
    ParamSpec,
    Protocol,
    TypeAlias,
    TypeVar,
    cast,
    overload,
    runtime_checkable,
    Literal,
    TypeGuard,
    Generic,
)

from astream import NoValue, SentinelType, ensure_async_iterator, ensure_coro_fn, NoValueT

_T = TypeVar("_T")
_U = TypeVar("_U")

_CoroT: TypeAlias = Coroutine[Any, Any, _T]

_P = ParamSpec("_P")

_KT = TypeVar("_KT", contravariant=True)
_VT = TypeVar("_VT", covariant=True)

_ItemAndFut: TypeAlias = Future[tuple[_T, "_ItemAndFut[_T]"]]


@runtime_checkable
class SupportsGetItem(Protocol[_KT, _VT]):
    """A protocol for objects that support `__getitem__`."""

    @abstractmethod
    def __getitem__(self, key: _KT) -> _VT:
        ...


async def aenumerate(iterable: AsyncIterable[_T], start: int = 0) -> AsyncIterator[tuple[int, _T]]:
    """An asynchronous version of `enumerate`."""
    async for item in iterable:
        yield start, item
        start += 1


async def agetitem(
    iterable: AsyncIterable[SupportsGetItem[_KT, _VT]],
    key: _KT,
) -> AsyncIterator[_VT]:
    """An asynchronous version of `getitem`."""
    async for item in iterable:
        yield item[key]


async def agetattr(
    iterable: AsyncIterable[object],
    name: str,
) -> AsyncIterator[Any]:
    """An asynchronous version of `getattr`."""
    async for item in iterable:
        yield getattr(item, name)


def afilter(
    fn: Callable[[_T], Coroutine[Any, Any, bool]] | Callable[[_T], bool],
    iterable: AsyncIterable[_T] | Iterable[_T],
) -> AsyncIterator[_T]:
    """An asynchronous version of `filter`."""

    async def _filter() -> AsyncIterator[_T]:
        _fn = ensure_coro_fn(fn)
        _iterable = ensure_async_iterator(iterable)
        async for item in _iterable:
            if await _fn(item):
                yield item

    return _filter()


def atee(
    iterable: AsyncIterable[_T] | Iterable[_T],
    n: int = 2,
) -> tuple[AsyncIterator[_T], ...]:
    """An asynchronous version of `tee`."""

    create_future = asyncio.get_running_loop().create_future

    async def _tee_feeder() -> None:
        async for item in ensure_async_iterator(iterable):
            ks = tuple(futs.keys())
            for fut in ks:
                tee = futs.pop(fut)
                next_fut = create_future()
                futs[next_fut] = tee
                fut.set_result((item, next_fut))
        for fut in futs:
            fut.set_exception(StopAsyncIteration)

    async def _tee(next_fut: _ItemAndFut[_T]) -> AsyncIterator[_T]:
        while True:
            try:
                item, next_fut = await next_fut
            except StopAsyncIteration:
                break
            yield item

    futs = {(f := create_future()): _tee(f) for _ in range(n)}
    asyncio.create_task(_tee_feeder())
    return tuple(futs.values())


@overload
def amap(
    fn: Callable[[_T], _CoroT[_U]], iterable: AsyncIterable[_T] | Iterable[_T]
) -> AsyncIterator[_U]:
    ...


@overload
def amap(fn: Callable[[_T], _U], iterable: AsyncIterable[_T] | Iterable[_T]) -> AsyncIterator[_U]:
    ...


def amap(
    fn: Callable[[_T], Coroutine[Any, Any, _U]] | Callable[[_T], _U],
    iterable: AsyncIterable[_T] | Iterable[_T],
) -> AsyncIterator[_U]:
    """An asynchronous version of `map`."""

    async def _map() -> AsyncIterator[_U]:
        _fn: Callable[[_T], _CoroT[_U]] = ensure_coro_fn(fn)
        _iterable = ensure_async_iterator(iterable)
        async for item in _iterable:
            yield await _fn(item)

    return _map()


@overload
def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: AsyncIterable[Iterable[_T]],
) -> AsyncIterator[_U]:
    ...


@overload
def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: AsyncIterable[AsyncIterable[_T]],
) -> AsyncIterator[_U]:
    ...


@overload
def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: Iterable[Iterable[_T]],
) -> AsyncIterator[_U]:
    ...


@overload
def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: Iterable[AsyncIterable[_T]],
) -> AsyncIterator[_U]:
    ...


def aflatmap(
    fn: Callable[[AsyncIterable[_T]], AsyncIterable[_U]],
    iterable: AsyncIterable[Iterable[_T]]
    | AsyncIterable[AsyncIterable[_T]]
    | Iterable[Iterable[_T]]
    | Iterable[AsyncIterable[_T]],
) -> AsyncIterator[_U]:
    async def _flatmap() -> AsyncIterator[_U]:
        _fn = ensure_coro_fn(fn)
        _iterable = ensure_async_iterator(iterable)
        async for item in _iterable:
            fn_iter = await _fn(item)
            async for subitem in ensure_async_iterator(fn_iter):
                yield subitem

    return _flatmap()


async def arange_delayed(
    start: int,
    stop: int | None = None,
    step: int = 1,
    delay: timedelta | float = timedelta(seconds=0.2),
) -> AsyncIterator[int]:
    """An asynchronous version of `range` with a delay between each item."""
    _delay = delay.total_seconds() if isinstance(delay, timedelta) else delay
    if stop is None:
        stop = start
        start = 0
    for i in range(start, stop, step):
        yield i
        await asyncio.sleep(_delay)


async def arange_delayed_random(
    start: int,
    stop: int | None = None,
    step: int = 1,
    delay_min: timedelta | float = timedelta(seconds=0.02),
    delay_max: timedelta | float = timedelta(seconds=0.3),
    rate_variance: float = 0.1,
) -> AsyncIterator[int]:
    """An asynchronous version of `range` with a random delay between each item."""
    _delay_min = delay_min.total_seconds() if isinstance(delay_min, timedelta) else delay_min
    _delay_max = delay_max.total_seconds() if isinstance(delay_max, timedelta) else delay_max
    rate = 1 / (_delay_min + _delay_max) / 2
    rate_variance = rate * rate_variance

    if stop is None:
        stop = start
        start = 0
    for i in range(start, stop, step):
        yield i
        await asyncio.sleep(
            random.uniform(
                max(_delay_min, rate - rate_variance),
                min(_delay_max, rate + rate_variance),
            )
        )
        rate = rate + random.uniform(-rate_variance, rate_variance)


async def arange_delayed_sine(
    start: int,
    stop: int | None = None,
    step: int = 1,
    delay_min: timedelta | float = timedelta(seconds=0.02),
    delay_max: timedelta | float = timedelta(seconds=0.3),
    rate: timedelta | float = timedelta(seconds=2),
) -> AsyncIterator[int]:
    """An asynchronous version of `range` with a random delay between each item."""
    _delay_min = delay_min.total_seconds() if isinstance(delay_min, timedelta) else delay_min
    _delay_max = delay_max.total_seconds() if isinstance(delay_max, timedelta) else delay_max
    _rate = rate.total_seconds() if isinstance(rate, timedelta) else rate

    delay_range = _delay_max - _delay_min

    if stop is None:
        stop = start
        start = 0

    for i in range(start, stop, step):
        yield i
        delay = _delay_min + delay_range * (math.sin(i / _rate) + 1) / 2
        await asyncio.sleep(delay)


async def arange(start: int, stop: int | None = None, step: int = 1) -> AsyncIterator[int]:
    """An asynchronous version of `range`.

    Args:
        start: The start of the range.
        stop: The end of the range.
        step: The step of the range.

    Yields:
        The next item in the range.

    Examples:
        >>> async def main():
        ...     async for i in arange(5):
        ...         print(i)
        >>> asyncio.run(main())
        0
        1
        2
        3
        4
    """
    async for i in arange_delayed(start, stop, step, delay=0):
        yield i


# class MergedAsyncIterator(Generic[_T], AsyncIterator[_T]):
#     """A protocol for a merged stream of items."""
#
#     merged_iterables: tuple[Iterable[_T] | AsyncIterable[_T], ...]
#
#     def __init__(self, *streams: Iterable[_T] | AsyncIterable[_T]) -> None:
#         self.merged_iterables = streams
#
#         self._futs: dict[asyncio.Future[_T], AsyncIterator[_T]] = {}
#         for it in self.merged_iterables:
#             async_it = ensure_async_iterator(it)
#             fut = asyncio.ensure_future(anext(async_it))
#             self._futs[fut] = async_it
#
#     def __aiter__(self) -> MergedAsyncIterator[_T]:
#         return self
#
#     async def __anext__(self) -> _T:
#         if not self._futs:
#             raise StopAsyncIteration
#         done, _ = await asyncio.wait(self._futs, return_when=asyncio.FIRST_COMPLETED)
#         for done_fut in done:
#             try:
#                 item = done_fut.result()
#                 fut = asyncio.ensure_future(anext(self._futs[done_fut]))
#                 self._futs[fut] = self._futs[done_fut]
#                 return item
#             except StopAsyncIteration:
#                 return await anext(self)
#             finally:
#                 if done_fut in self._futs:
#                     del self._futs[done_fut]
#         raise StopAsyncIteration


def amerge(*async_iters: Iterable[_T] | AsyncIterable[_T]) -> AsyncIterator[_T]:
    """Merge multiple iterables or async iterables into one, yielding items as they are received.

    Args:
        async_iters: The async iterators to merge.

    Yields:
        Items from the async iterators, as they are received.

    Examples:
        >>> async def a():
        ...     for i in range(3):
        ...         await asyncio.sleep(0.07)
        ...         yield i
        >>> async def b():
        ...     for i in range(100, 106):
        ...         await asyncio.sleep(0.03)
        ...         yield i
        >>> async def demo_amerge():
        ...     async for item in amerge(a(), b()):
        ...         print(item)
        >>> asyncio.run(demo_amerge())
        100
        101
        0
        102
        103
        1
        104
        105
        2
    """

    async def _inner() -> AsyncIterator[_T]:
        futs: dict[asyncio.Future[_T], AsyncIterator[_T]] = {}
        for it in async_iters:
            async_it = ensure_async_iterator(it)
            fut = asyncio.ensure_future(anext(async_it))
            futs[fut] = async_it

        while futs:
            done, _ = await asyncio.wait(futs, return_when=asyncio.FIRST_COMPLETED)
            for done_fut in done:
                try:
                    yield done_fut.result()
                except StopAsyncIteration:
                    pass
                else:
                    fut = asyncio.ensure_future(anext(futs[done_fut]))
                    futs[fut] = futs[done_fut]
                finally:
                    del futs[done_fut]

    return _inner()


async def ascan(
    fn: Callable[[_T, _U], Coroutine[Any, Any, _T]] | Callable[[_T, _U], _T],
    iterable: AsyncIterable[_U],
    initial: _T | NoValueT = NoValue,
) -> AsyncIterator[_T]:
    """An asynchronous version of `scan`.

    Args:
        fn: The function to scan with.
        iterable: The iterable to scan.
        initial: The initial value to scan with.

    Yields:
        The scanned value.

    Examples:
        >>> async def demo_ascan():
        ...     async for it in ascan(lambda a, b: a + b, arange(5)):
        ...         print(it)
        >>> asyncio.run(demo_ascan())
        0
        1
        3
        6
        10
    """
    _fn_async = ensure_coro_fn(fn)
    _it_async = ensure_async_iterator(iterable)

    if isinstance(initial, NoValueT):
        initial = await anext(_it_async)  # type: ignore
    crt = cast(_T, initial)

    yield crt

    async for item in _it_async:
        crt = await _fn_async(crt, item)  # type: ignore
        yield crt


@overload
def aflatten(iterable: AsyncIterator[Iterable[_T]]) -> AsyncIterator[_T]:
    ...


@overload
def aflatten(iterable: AsyncIterator[AsyncIterable[_T]]) -> AsyncIterator[_T]:
    ...


async def aflatten(
    iterable: AsyncIterator[Iterable[_T]] | AsyncIterator[AsyncIterable[_T]],
) -> AsyncIterator[_T]:
    """Unpacks an async iterator of iterables or async iterables into a flat async iterator."""
    async for item in iterable:
        async for subitem in ensure_async_iterator(item):
            yield subitem


async def aconcatenate(
    *iterables: Iterable[_T] | AsyncIterable[_T],
) -> AsyncIterator[_T]:
    """Concatenates multiple async iterators, yielding all items from the first, then all items
    from the second, etc.
    """
    for iterable in iterables:
        async for item in ensure_async_iterator(iterable):
            yield item


async def arepeat(iterable: Iterable[_T] | AsyncIterable[_T], times: int) -> AsyncIterator[_T]:
    """Repeats an async iterator `times` times."""
    tees = atee(ensure_async_iterator(iterable), times)
    for tee in tees:
        async for item in tee:
            yield item


async def azip(
    *iterables: Iterable[_T] | AsyncIterable[_T],
) -> AsyncIterator[tuple[_T, ...]]:
    """An asynchronous version of `zip`.

    Args:
        *iterables: The iterables to zip.

    Yields:
        The zipped values.

    Examples:
        >>> async def demo_azip():
        ...     async for it in azip(arange(5), arange(5, 10)):
        ...         print(it)
        >>> asyncio.run(demo_azip())
        (0, 5)
        (1, 6)
        (2, 7)
        (3, 8)
        (4, 9)
    """
    async_iterables = tuple(ensure_async_iterator(it) for it in iterables)
    while True:
        try:
            items = await asyncio.gather(*(anext(it) for it in async_iterables))
        except StopAsyncIteration:
            break
        else:
            yield tuple(items)


@overload
def azip_longest(
    *iterables: Iterable[_T] | AsyncIterable[_T],
    fillvalue: None = ...,
) -> AsyncIterator[tuple[_T | None, ...]]:
    ...


@overload
def azip_longest(
    *iterables: Iterable[_T] | AsyncIterable[_T],
    fillvalue: _T = ...,
) -> AsyncIterator[tuple[_T, ...]]:
    ...


async def azip_longest(
    *iterables: Iterable[_T] | AsyncIterable[_T],
    fillvalue: _T | None = None,
) -> AsyncIterator[tuple[_T | None, ...]]:
    """An asynchronous version of `zip_longest`."""
    async_iterables = [ensure_async_iterator(it) for it in iterables]
    while True:
        items = await asyncio.gather(*(anext(it, NoValue) for it in async_iterables))
        if all(item is NoValue for item in items):
            break
        yield tuple(item if item is not NoValue else fillvalue for item in items)


async def bytes_stream_split_separator(
    stream: AsyncIterable[bytes],
    separator: bytes = b"\n",
    strip_characters: tuple[bytes, ...] = (b"\r", b"\n", b"\t", b" ", b"\x00", b"\x0b", b"\x0c"),
) -> AsyncIterator[bytes]:
    """Splits a stream of bytes by a separator.

    Args:
        stream: The stream of bytes.
        separator: The separator to split by.
        strip_characters: The characters to strip from the end/beginning of the split.

    Yields:
        The split bytes.

    Examples:
        >>> from astream import stream
        >>> async def demo_bytes_stream_split_separator():
        ...     async for it in bytes_stream_split_separator(
        ...         stream([b"hello", b"world", b"!"]),
        ...         b"o",
        ...     ):
        ...         print(it)
        >>> asyncio.run(demo_bytes_stream_split_separator())
        b'hell'
        b'w'
        b'rld!'
    """

    # b"\x00" is a null byte, which is used to terminate strings in C.
    # b"\x0b" and b"\x0c" are vertical and form feed characters, which are used to terminate
    # strings in some languages. They are also used to separate pages in some terminal emulators.

    strip_characters_str = b"".join(strip_characters)
    buf = bytearray()
    async for chunk in stream:
        buf.extend(chunk)
        while True:
            line, sep, remaining = buf.partition(separator)
            if sep:
                yield bytes(line.strip(strip_characters_str))
                buf = bytearray(remaining)
            else:
                break
    yield bytes(buf.strip(strip_characters_str))


_AsyncIterableT = TypeVar("_AsyncIterableT", bound=AsyncIterable[Any])


class AsyncIteratorWithExceptionHandler(Generic[_T], AsyncIterable[_T]):
    """An async iterator that handles exceptions.

    Args:
        iterable: The async iterable to wrap.
        exception_handler: The exception handler to use.

    The handler is called with the exception and the async iterator. If it returns a value, that
    value is yielded. If it returns `NoValue`, no value is yielded, but the iteration continues.
    If it raises an exception, that exception is raised.
    """

    def __init__(
        self,
        iterable: AsyncIterable[_T] | Iterable[_T],
        exception_handler: Callable[
            [BaseException, Iterable[_T] | AsyncIterable[_T]], _T | NoValueT
        ]
        | Callable[[BaseException, Iterable[_T] | AsyncIterable[_T]], _CoroT[_T | NoValueT]],
    ) -> None:
        self._iterator = aiter(ensure_async_iterator(iterable))
        self._iterable = iterable

        _async_handler = ensure_coro_fn(exception_handler)
        self._exception_handler = _async_handler

    def __aiter__(self) -> AsyncIteratorWithExceptionHandler[_T]:
        return self

    async def _anext(self) -> _T:
        try:
            return await anext(self._iterator)
        except StopAsyncIteration:
            print("StopAsyncIteration raised")
            raise StopAsyncIteration
        except BaseException as exc:
            if isinstance(exc, StopAsyncIteration):
                print("StopAsyncIteration in BaseException")
                raise StopAsyncIteration
            else:
                result = await self._exception_handler(exc, self._iterable)
                if isinstance(result, NoValueT):
                    return await self._anext()
                else:
                    return cast(_T, result)

    async def __anext__(self) -> _T:
        return await self._anext()


@overload
def with_exc_handler(
    iterable: Iterable[_T] | AsyncIterable[_T],
    handler: Callable[[BaseException, Iterable[_T] | AsyncIterable[_T]], _CoroT[_T | NoValueT]],
) -> AsyncIterator[_T]:
    ...


@overload
def with_exc_handler(
    iterable: Iterable[_T] | AsyncIterable[_T],
    handler: Callable[[BaseException, Iterable[_T] | AsyncIterable[_T]], _T | NoValueT],
) -> AsyncIterator[_T]:
    ...


def with_exc_handler(
    iterable: Iterable[_T] | AsyncIterable[_T],
    handler: Callable[[BaseException, Iterable[_T] | AsyncIterable[_T]], _CoroT[_T | NoValueT]]
    | Callable[[BaseException, Iterable[_T] | AsyncIterable[_T]], _T | NoValueT],
) -> AsyncIterator[_T]:
    """Wraps an async iterator with an exception handler.

    The handler is called with the exception and the async iterator. If it returns a value, that
    value is yielded. If it returns `NoValue`, no value is yielded, but the iteration continues.
    If it raises an exception, that exception is raised.

    Args:
        iterable: The async iterator.
        handler: The exception handler.

    Yields:
        The yielded values.

    Examples:
        >>> async def demo_with_exc_handler():
        ...     async for it in with_exc_handler(arange(5), print):
        ...         print(it)
        >>> asyncio.run(demo_with_exc_handler())
        0
        1
        2
        3
        4
    """
    # return AsyncIteratorWithExceptionHandler(iterable, handler)
    _async_handler = ensure_coro_fn(handler)

    async def _aiter() -> AsyncIterator[_T]:
        ait = aiter(iterable)
        while True:
            try:
                yield await anext(ait)
            except (StopAsyncIteration, StopIteration, GeneratorExit):
                raise
            except BaseException as exc:
                result = await _async_handler(exc, iterable)
                if isinstance(result, NoValueT):
                    continue
                else:
                    yield cast(_T, result)

    return _aiter()


__all__ = (
    "aconcatenate",
    "aenumerate",
    "afilter",
    "aflatmap",
    "aflatten",
    "agetattr",
    "agetitem",
    "amap",
    "amerge",
    "arange",
    "arange_delayed",
    "arange_delayed_random",
    "arange_delayed_sine",
    "arepeat",
    "ascan",
    "atee",
    "azip",
    "azip_longest",
    "bytes_stream_split_separator",
    "with_exc_handler",
)

if __name__ == "__main__":

    async def demo() -> None:
        async for i in aenumerate(arange(10, 20)):
            print(i)

        async for i in aenumerate(arange(10), 5):
            print(i)

        (a, b), c = atee(arange(10, 15), 2), arange(15, 25)
        async for j in a:
            print("a", j)
        async for j in b:
            print("b", j)
        async for j in c:
            print("c", j)

        (a, b), c = atee(arange(10, 15), 2), arange(15, 25)
        async for tup in azip_longest(a, b, c, fillvalue=-1):
            print(tup)

    asyncio.run(demo())
