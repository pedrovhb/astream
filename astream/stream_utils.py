from __future__ import annotations

import abc
import asyncio
import heapq
import itertools

import math
import random
from abc import abstractmethod
from asyncio import Future, Queue, Task
from collections import deque
from datetime import timedelta
from functools import partial
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    cast,
    Coroutine,
    Iterable,
    Iterator,
    Literal,
    overload,
    ParamSpec,
    Protocol,
    runtime_checkable,
    TypeAlias,
    TypeVar,
)

from .pure import aflatten as _pure_aflatten

from .sentinel import _NoValueT, Sentinel
from .stream import FnTransformer, Stream, stream, transformer, Transformer
from .utils import create_future, ensure_async_iterator, ensure_coroutine_function

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


@overload
def atee(
    iterable: AsyncIterable[_T] | Iterable[_T], n: Literal[2]
) -> tuple[Stream[_T], Stream[_T]]:
    ...


@overload
def atee(
    iterable: AsyncIterable[_T] | Iterable[_T], n: Literal[3]
) -> tuple[Stream[_T], Stream[_T], Stream[_T]]:
    ...


@overload
def atee(
    iterable: AsyncIterable[_T] | Iterable[_T], n: Literal[4]
) -> tuple[Stream[_T], Stream[_T], Stream[_T], Stream[_T]]:
    ...


@overload
def atee(iterable: AsyncIterable[_T] | Iterable[_T], n: int) -> tuple[Stream[_T], ...]:
    ...


def atee(iterable: AsyncIterable[_T] | Iterable[_T], n: int = 2) -> tuple[Stream[_T], ...]:
    """An asynchronous version of `tee`."""

    # futs: dict[Future[None], Queue[_T]] = {create_future(): Queue() for _ in range(n)}
    queues: tuple[Queue[_T], ...] = tuple(Queue() for _ in range(n))

    # Initially future is None so we can run atee before there is a running event loop
    done_future: Future[None] | None = None
    feeder_task: Task[None] | None = None

    async def _tee_feeder() -> None:
        nonlocal done_future
        assert done_future is not None
        _iterable = ensure_async_iterator(iterable)
        async for item in _iterable:
            for queue in queues:
                queue.put_nowait(item)
        done_future.set_result(None)

    @stream
    async def _tee(queue: Queue[_T]) -> AsyncIterator[_T]:
        nonlocal done_future, feeder_task
        if done_future is None:
            done_future = create_future()
        if feeder_task is None:
            feeder_task = asyncio.create_task(_tee_feeder())

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
    initial: _T | _NoValueT = Sentinel.NoValue,
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
    _it_async: AsyncIterator[_U] = ensure_async_iterator(iterable)

    if initial is Sentinel.NoValue:
        initial = cast(_T, await anext(_it_async))
    crt = initial

    yield crt

    async for item in _it_async:
        crt = await _fn_async(crt, item)
        yield crt


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
        items = await asyncio.gather(*(anext(it, Sentinel.NoValue) for it in async_iterables))
        if all(item is Sentinel.NoValue for item in items):
            break
        yield tuple(item if item is not Sentinel.NoValue else fillvalue for item in items)


@transformer
async def bytes_stream_split_separator(
    stream: AsyncIterable[bytes],
    separator: bytes = b"\n",  # todo - match behavior of separator to Python's universal newlines
    keep_separator: bool = False,
) -> AsyncIterator[bytes]:
    """Splits a stream of bytes by a separator.

    Args:
        stream: The stream of bytes.
        separator: The separator to split by.
        keep_separator: Whether to keep the separator in the output.

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

    buf = bytearray()
    async for chunk in stream:
        buf.extend(chunk)
        while True:
            line, sep, remaining = buf.partition(separator)
            if sep:
                if keep_separator:
                    yield bytes(line) + sep
                else:
                    yield bytes(line)
                buf = bytearray(remaining)
            else:
                break
    yield bytes(buf)


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


@stream
def repeat_value(value: _T, n: int | None = None) -> Iterator[_T]:
    """Repeats a value `n` times.

    Args:
        value: The value to repeat.
        n: The number of times to repeat the value. If `None`, repeats forever.

    Examples:
        >>> async def demo_repeat_value() -> None:
        ...     async for item in repeat_value(3, 2):
        ...         print(item)
        >>> asyncio.run(demo_repeat_value())
        3
        3
    """
    if n is None:
        while True:
            yield value
    else:
        for _ in range(n):
            yield value


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


@transformer
async def delay(async_iterator: AsyncIterator[_T], delay: float) -> AsyncIterator[_T]:
    """Delay each item in the stream by `delay` seconds.

    Args:
        delay: The number of seconds to delay each item.

    Examples:
        >>> async def demo_delay() -> None:
        ...     async for item in range(3) / delay(0.5):
        ...         print(item)
        >>> asyncio.run(demo_delay())
        0
        1
        2
    """
    async for item in async_iterator:
        await asyncio.sleep(delay)
        yield item


@stream
async def interleave(
    *iterables: Iterable[_T] | AsyncIterable[_T],
) -> AsyncIterator[_T]:
    """Interleave items from multiple async iterables.

    Args:
        *async_iterators: The async iterables to interleave.

    Returns:
        An async iterable of the interleaved items.

    Examples:
        >>> async def demo_interleave() -> None:
        ...     async for item in interleave(range(2), range(2, 4)):
        ...         print(item)
        >>> asyncio.run(demo_interleave())
        0
        2
        1
        3
    """
    _async_iterators = [ensure_async_iterator(i) for i in iterables]
    while True:
        for async_iterator in _async_iterators:
            try:
                yield await anext(async_iterator)
            except StopAsyncIteration:
                return


@transformer
async def interleave_with(
    async_iterator: AsyncIterator[_T],
    *iterables: Iterable[_U] | AsyncIterable[_U],
    stop_on_first_empty: bool = False,
) -> AsyncIterator[_T | _U]:
    """Interleave items from multiple async iterables.

    Args:
        *async_iterators: The async iterables to interleave.

    Returns:
        An async iterable of the interleaved items.

    Examples:
        >>> async def demo_interleave_with() -> None:
        ...     async for item in range(2) / interleave_with(range(2, 4)):
        ...         print(item)
        >>> asyncio.run(demo_interleave_with())
        0
        2
        1
        3
    """
    _async_iterators: set[AsyncIterator[_T]] = {async_iterator}
    _async_iterators.update(ensure_async_iterator(it) for it in iterables)  # type: ignore
    while True:
        for ait in _async_iterators:
            try:
                yield await anext(ait)
            except StopAsyncIteration:
                if stop_on_first_empty:
                    return
                _async_iterators.remove(ait)


@transformer
async def interleave_with_values(
    async_iterator: AsyncIterator[_T],
    *values: _T,
) -> AsyncIterator[_T]:
    """Interleave items from multiple async iterables.

    Args:
        *async_iterators: The async iterables to interleave.

    Returns:
        An async iterable of the interleaved items.

    Examples:
        >>> async def demo_interleave_with_values() -> None:
        ...     async for item in range(2) / interleave_with_values(2, 3):
        ...         print(item)
        >>> asyncio.run(demo_interleave_with_values())
        0
        2
        1
        3
    """
    async for i in interleave(async_iterator, *(itertools.repeat(v) for v in values)):
        yield i


@transformer
async def call_and_passthrough(
    async_iterator: AsyncIterator[_T],
    func: Callable[[_T], Any] | Callable[[_T], Coroutine[Any, Any, Any]],
) -> AsyncIterator[_T]:
    """Call a function on each item in the stream and yield the item.

    Args:
        func: The function to call on each item.

    Examples:
        >>> async def demo_call_and_passthrough() -> None:
        ...     async for item in range(3) / call_and_passthrough(print):
        ...         print(item)
        >>> asyncio.run(demo_call_and_passthrough())
        0
        0
        1
        1
        2
        2
    """
    _func = ensure_coroutine_function(func)
    async for item in async_iterator:
        await _func(item)
        yield item


_SupportsLessThanT = TypeVar("_SupportsLessThanT", bound="SupportsLessThan")


@runtime_checkable
class SupportsLessThan(Protocol):
    __slots__ = ()

    @abc.abstractmethod
    def __lt__(self: _SupportsLessThanT, other: _SupportsLessThanT) -> bool:
        ...


def identity(x: _T) -> _T:
    return x


@transformer
async def top_k(
    async_iterator: AsyncIterator[_T],
    k: int,
    key: Callable[[_T], Any] | None = None,
    on_tie: Literal["keep_first", "keep_last"] = "keep_first",
) -> AsyncIterator[tuple[_T, ...]]:
    """Yield the top `k` items in the stream.

    Args:
        k: The number of items to yield.
        key: The key to sort by.

    Examples:
        >>> async def demo_top_k() -> None:
        ...     async for item in range(10) / top_k(3):
        ...         print(item)
        >>> asyncio.run(demo_top_k())
        9
        8
        7
    """
    heap: list[tuple[_T | Any, int, _T]] = []
    insertion_counter = 0

    async for item in async_iterator:

        insertion_counter += 1
        key_value = key(item) if key else item
        if on_tie == "keep_first":
            idx = (key_value, insertion_counter, item)
        else:
            idx = (key_value, -insertion_counter, item)

        if len(heap) < k:
            heapq.heappush(heap, idx)
        else:
            heapq.heappushpop(heap, idx)

        top = heapq.nlargest(k, heap)
        yield tuple(elem[2] for elem in top)


@transformer
async def partition_by_element(
    async_iterator: AsyncIterator[_T],
    separator: _T,
) -> AsyncIterator[list[_T]]:
    """Group items into partitions separated by a specified element.

    Args:
        async_iterator: The async iterable to group.
        separator: The element to use as a separator.

    Examples:
        >>> async def demo_group_partitions() -> None:
        ...     async for item in range(10) / partition_by_element(5):
        ...         print(item)
        >>> asyncio.run(demo_group_partitions())
        [0, 1, 2, 3, 4]
        [5, 6, 7, 8, 9]
    """
    partition: list[_T] = []
    async for item in async_iterator:
        if item == separator:
            yield partition
            partition = []
        else:
            partition.append(item)
    if partition:
        yield partition


aflatten = transformer(_pure_aflatten)

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
    "call_and_passthrough",
    "top_k",
    "partition_by_element",
)
