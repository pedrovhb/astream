from __future__ import annotations

import asyncio
from collections import deque
from functools import partial
import random
from abc import abstractmethod
from asyncio import Future, Queue
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
)

import math

from .stream import transformer, FnTransformer, Transformer, Stream, stream
from .utils import (
    NoValue,
    ensure_async_iterator,
    ensure_coroutine_function,
    NoValueT,
    create_future,
)

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


@transformer
async def aenumerate(iterable: AsyncIterable[_T], start: int = 0) -> AsyncIterator[tuple[int, _T]]:
    """An asynchronous version of `enumerate`."""
    async for item in iterable:
        yield start, item
        start += 1


@transformer
async def agetitem(
    iterable: AsyncIterable[SupportsGetItem[_KT, _VT]],
    key: _KT,
) -> AsyncIterator[_VT]:
    """An asynchronous version of `getitem`."""
    async for item in iterable:
        yield item[key]


@transformer
async def agetattr(
    iterable: AsyncIterable[object],
    name: str,
) -> AsyncIterator[Any]:
    """An asynchronous version of `getattr`."""
    async for item in iterable:
        yield getattr(item, name)


def atee(
    iterable: AsyncIterable[_T] | Iterable[_T],
    n: int = 2,
) -> tuple[AsyncIterator[_T], ...]:
    """An asynchronous version of `tee`."""

    # futs: dict[Future[None], Queue[_T]] = {create_future(): Queue() for _ in range(n)}
    queues: tuple[Queue[_T], ...] = tuple(Queue() for _ in range(n))
    done_future: Future[None] = create_future()

    async def _tee_feeder() -> None:
        _iterable = ensure_async_iterator(iterable)
        async for item in _iterable:
            for queue in queues:
                queue.put_nowait(item)
        done_future.set_result(None)

    async def _tee(queue: Queue[_T]) -> AsyncIterator[_T]:
        while True:
            done, pending = await asyncio.wait(
                (done_future, get_task := asyncio.create_task(queue.get())),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if get_task in done:
                item = await get_task
                queue.task_done()
                yield item

            if done_future in done:
                while not queue.empty():
                    item = queue.get_nowait()
                    queue.task_done()
                    yield item
                break

    asyncio.create_task(_tee_feeder())
    return tuple(_tee(queue) for queue in queues)


@stream
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


@stream
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


@stream
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


def arange(start: int, stop: int | None = None, step: int = 1) -> Stream[int]:
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
    return arange_delayed(start, stop, step, delay=0)


@stream
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


@transformer
async def ascan(
    iterable: AsyncIterable[_U],
    fn: Callable[[_T, _U], Coroutine[Any, Any, _T]] | Callable[[_T, _U], _T],
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
    _fn_async = cast(Callable[[_T, _U], Coroutine[Any, Any, _T]], ensure_coroutine_function(fn))
    _it_async = ensure_async_iterator(iterable)

    if isinstance(initial, NoValueT):
        initial = await anext(_it_async)
    crt = cast(_T, initial)

    yield crt

    async for item in _it_async:
        crt = await _fn_async(crt, item)
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


@transformer
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
        >>> from astream import Stream
        >>> async def demo_bytes_stream_split_separator():
        ...     s = Stream([b"hello", b"world", b"!"])
        ...     async for it in s / bytes_stream_split_separator(b"o"):
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


__all__ = (
    "aconcatenate",
    "aenumerate",
    "aflatten",
    "agetattr",
    "agetitem",
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
)


def int_to_str(x: int) -> float:
    return str(x)


## new style


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
        >>> async def demo_nwise() -> None:
        ...     async for item in Stream(range(4)).transform(nwise(2)):
        ...         print(item)
        >>> asyncio.run(demo_nwise())
        (0, 1)
        (1, 2)
        (2, 3)
    """
    return FnTransformer(partial(_nwise, n=n))


# @transformer
# async def flatten(
#     async_iterator: AsyncIterator[Iterable[_T] | AsyncIterable[_T]],
# ) -> AsyncIterator[_T]:
#     """Flatten an async iterable of async iterables.
#
#     Args:
#         async_iterator: The async iterable to flatten.
#
#     Returns:
#         An async iterable of the flattened items.
#
#     Examples:
#         >>> async def demo_flatten() -> None:
#         ...     async for item in Stream([range(2), range(2, 4)]).transform(flatten()):
#         ...         print(item)
#         >>> asyncio.run(demo_flatten())
#         0
#         1
#         2
#         3
#     """
#     async for item in async_iterator:
#         sub_iter = ensure_async_iterator(item)
#         async for sub_item in sub_iter:
#             yield sub_item


@transformer
async def repeat(async_iterator: AsyncIterator[_T], n: int) -> AsyncIterator[_T]:
    """Repeats each item in the stream `n` times.

    Args:
        async_iterator: The async iterable to repeat.
        n: The number of times to repeat each item.

    Examples:
        >>> async def demo_repeat() -> None:
        ...     async for item in range(3) / repeat(2):
        ...         print(item)
        >>> asyncio.run(demo_repeat())
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
    """Take the first `n` items from the stream.

    Examples:
        >>> async def demo_take() -> None:
        ...     async for item in range(3) / take(2):
        ...         print(item)
        >>> asyncio.run(demo_take())
        0
        1
    """
    for _ in range(n):
        yield await anext(async_iterator)


@transformer
async def drop(async_iterator: AsyncIterator[_T], n: int) -> AsyncIterator[_T]:
    """Drop the first `n` items from the stream.

    Examples:
        >>> async def demo_drop() -> None:
        ...     async for item in range(3) / drop(2):
        ...         print(item)
        >>> asyncio.run(demo_drop())
        2
    """
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
        >>> async def demo_immediately_unique_1() -> None:
        ...     async for item in range(5) / repeat(3) / immediately_unique():
        ...         print(item)
        >>> asyncio.run(demo_immediately_unique_1())
        0
        1
        2
        3
        4
        >>> async def demo_immediately_unique_2() -> None:
        ...     async for item in range(50) / immediately_unique(int.bit_length):
        ...         print(item)
        >>> asyncio.run(demo_immediately_unique_2())
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
        >>> async def demo_unique_1() -> None:
        ...     async for item in Stream(range(5)) / repeat(3) / unique():
        ...         print(item)
        >>> asyncio.run(demo_unique_1())
        0
        1
        2
        3
        4
        >>> async def demo_unique_2() -> None:
        ...     async for item in Stream(range(50, 103, 6)) / unique(lambda x: str(x)[0]):
        ...         print(item)
        >>> asyncio.run(demo_unique_2())
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
