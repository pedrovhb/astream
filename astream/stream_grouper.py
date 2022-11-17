from __future__ import annotations

import asyncio
import functools
from asyncio import Future, Task
from collections import defaultdict
from enum import Enum
from typing import *

from astream import SentinelType, arange
from astream.closeable_queue import CloseableQueue
from astream.experimental.partializer import F
from astream.stream import Stream, StreamMappable
from astream.stream_utils import amerge, arange_delayed
from astream.utils import create_future, ensure_coro_fn

_T = TypeVar("_T")
_U = TypeVar("_U")
_KeyT = TypeVar("_KeyT")

_P = ParamSpec("_P")
Coro: TypeAlias = Coroutine[Any, Any, _T]
_GroupingFunctionT: TypeAlias = Union[Callable[[_T], _KeyT], Callable[[_T], Coro[_KeyT]]]

UnaryFn: TypeAlias = Union[Callable[[_T], _U], Callable[[_T], Coro[_U]]]

_DefaultT = NewType("_DefaultT", SentinelType)
Default = _DefaultT(SentinelType())  # noqa


class StreamGrouper(Generic[_T, _KeyT], Mapping[_KeyT, Stream[_T]]):

    _grouping_task: Task[None] | None
    _akeys_stream: Stream[_KeyT] | None

    @staticmethod
    def _starts_async_iteration(fn: Callable[_P, _U]) -> Callable[_P, _U]:
        """Wraps a function so that it starts the grouping consumer task, if not already started.

        This is necessary to allow creating the StreamGrouper instance outside the context
        of a running event loop, as creating the consumer task requires an event loop.
        """

        @functools.wraps(fn)
        def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _U:
            instance = cast(StreamGrouper[_T, _KeyT], args[0])  # aka self
            if instance._grouping_task is None:
                instance._grouping_task = asyncio.create_task(instance._consume_source())
            return fn(*args, **kwargs)

        return cast(Callable[_P, _U], wrapped)

    def __init__(
        self,
        grouping_function: _GroupingFunctionT[_T, _KeyT],
        group_stream: AsyncIterable[_T],
    ) -> None:
        self._grouping_function = ensure_coro_fn(grouping_function)
        self._source_stream = aiter(group_stream)

        self._key_queues: dict[_KeyT, CloseableQueue[_T]] = {}
        self._key_streams: defaultdict[_KeyT, Future[Stream[_T]]]
        self._key_streams = defaultdict(create_future)

        self._new_key_queue: CloseableQueue[_KeyT] = CloseableQueue()
        self._akeys_stream = None

        self._grouping_task = None

    async def _consume_source(self) -> None:
        """Consumes the source stream and groups the items into the appropriate queues.

        We start this task when one of the functions decorated with _starts_async_iteration is
        first called; this allows creating the StreamGrouper instance outside the context
        of a running event loop. Had it been started in __init__, it would have raised an
        exception.
        """

        # Iterate over the source stream, grouping items into the appropriate queues
        async for item in self._source_stream:
            key = await self._grouping_function(item)
            if key not in self._key_queues:
                self._create_group(key)
            await self._key_queues[key].put(item)

        # After the source stream is exhausted, close all the queues
        for key in self._key_queues:
            self._key_queues[key].close()
        self._new_key_queue.close()

    def _create_group(self, key: _KeyT) -> None:
        """Creates the queue and stream for a given key."""
        self._key_queues[key] = CloseableQueue()
        self._key_streams[key].set_result(Stream(aiter(self._key_queues[key])))
        self._new_key_queue.put_nowait(key)

    @_starts_async_iteration
    def __getitem__(self, key: _KeyT) -> Stream[_T]:
        if not self._key_streams[key].done():
            raise KeyError(
                f"No stream for key {repr(key)}. To iterate on a "
                f"key that is yet to be created, use get_wait(key)."
            )
        return self._key_streams[key].result()

    @_starts_async_iteration
    def get_wait(self, key: _KeyT) -> Stream[_T]:
        """Returns a stream that will wait for the key to be created before yielding items."""

        async def _wait_yield() -> AsyncIterator[_T]:
            q = self._key_streams[key]
            if not q.done():
                await q
            async for it in q.result():
                yield it

        return Stream(_wait_yield())

    @_starts_async_iteration
    def akeys(self) -> Stream[_KeyT]:
        if self._akeys_stream is None:
            self._akeys_stream = Stream(self._new_key_queue)
        return self._akeys_stream

    def __contains__(self, key: object) -> bool:
        return key in self._key_queues

    def __len__(self) -> int:
        return len(self._key_queues)

    def __iter__(self) -> Iterator[_KeyT]:
        return iter(self._key_queues)


def _agroup_map(
    grouping_function: _GroupingFunctionT[_T, _KeyT],
    group_stream: AsyncIterable[_T],
    mapping_functions: Mapping[_KeyT | _DefaultT, UnaryFn[_T, _U]],
) -> Stream[_U]:
    """
    Groups items from the given stream according to the given grouping function, and applies
    the given mapping functions to the groups.

    If Default is used as a key, the corresponding mapping function will be applied to all
    items that do not match any other key. If Default is not used, items that do not match
    any key will be discarded.

    Args:
        grouping_function:
        group_stream:
        mapping_functions:

    Returns:

    """
    grouper = StreamGrouper(grouping_function, group_stream)
    output_queue = CloseableQueue[_U]()

    async def output_queue_filler(fill_key: _KeyT) -> None:
        _fn = mapping_functions.get(fill_key, mapping_functions.get(Default))
        _key_stream = grouper.get_wait(fill_key)

        if _fn is None:
            async for _ in _key_stream:
                pass
        else:
            _fn_async = ensure_coro_fn(_fn)
            async for it in _key_stream:
                mapped = await _fn_async(it)
                await output_queue.put(mapped)

    async def key_inserter() -> None:
        tasks = set()
        async for key in grouper.akeys():
            tasks.add(asyncio.create_task(output_queue_filler(key)))
        await asyncio.gather(*tasks)
        output_queue.close()

    asyncio.create_task(key_inserter())
    return Stream(aiter(output_queue))


def _apredicate_map(
    stream: AsyncIterable[_T],
    predicates_maps: Mapping[UnaryFn[_T, bool] | _DefaultT, UnaryFn[_T, _U]],
    stop_after_first_match: bool,
) -> Stream[_U]:
    """Maps items in a stream to a new value if the predicate returns True.

    Args:
        predicate: A function that takes an item from the stream and returns a boolean
            indicating whether the item should be mapped.
        stream: The stream to map.
        predicates_maps: A function that takes an item from the stream and returns a new value
            for that item. If the predicate returns False for an item, the item is not mapped.

    Returns:
        A stream that yields the mapped items.
    """
    output_queue = CloseableQueue[_U]()

    async_predicate_map = dict[
        SentinelType | Callable[[_T], Coro[bool]],
        Callable[[_T], Coro[_U]],
    ]()

    for pred, mapping_function in predicates_maps.items():
        if isinstance(pred, SentinelType):
            if pred is not Default:
                raise ValueError(f"Invalid sentinel value {pred!r} for predicate.")
            async_predicate_map[Default] = ensure_coro_fn(mapping_function)
        else:
            _pred = ensure_coro_fn(pred)
            async_predicate_map[_pred] = ensure_coro_fn(mapping_function)

    async def _predicate_checker(item: _T) -> None:
        predicates_matched = []
        for predicate, fn in async_predicate_map.items():

            if isinstance(pred_fn := predicate, SentinelType):
                predicate_true = True  # Default predicate always matches
            else:
                predicate_true = await pred_fn(item)
            if predicate_true:
                # await output_queue.put(await fn(item))
                predicates_matched.append(fn)
                if stop_after_first_match:
                    break
        if predicates_matched:
            for fn in predicates_matched:
                item = await fn(item)

            await output_queue.put(item)

    async def output_queue_filler() -> None:
        tasks = set()
        async for item in stream:
            tasks.add(asyncio.create_task(_predicate_checker(item)))
        await asyncio.gather(*tasks)
        await output_queue.join()
        output_queue.close()

    asyncio.create_task(output_queue_filler())
    return Stream(output_queue)


def apredicate_multi_map(
    mapping_functions: Mapping[UnaryFn[_T, bool] | _DefaultT, UnaryFn[_T, _U]],
) -> StreamMappable[_T, _U]:
    @functools.wraps(_apredicate_map)
    def _partial(stream: AsyncIterable[_T]) -> Stream[_U]:
        return _apredicate_map(stream, mapping_functions, stop_after_first_match=False)

    setattr(_partial, "__stream_map__", _partial)  # See: WithStream protocol
    return cast(StreamMappable[_T, _U], _partial)


def apredicate_map(
    mapping_functions: Mapping[UnaryFn[_T, bool] | _DefaultT, UnaryFn[_T, _U]]
) -> StreamMappable[_T, _U]:
    # @functools.wraps(_apredicate_map)
    def _partial(stream: AsyncIterable[_T]) -> Stream[_U]:
        return _apredicate_map(stream, mapping_functions, stop_after_first_match=True)

    setattr(_partial, "__stream_map__", _partial)  # See: WithStream protocol
    return cast(StreamMappable[_T, _U], _partial)


def agroup_map(
    grouping_function: _GroupingFunctionT[_T, _KeyT],
    mapping_functions: Mapping[_KeyT | _DefaultT, UnaryFn[_T, _U]],
) -> StreamMappable[_T, _U]:
    @functools.wraps(_agroup_map)
    def _partial(stream: AsyncIterable[_T]) -> Stream[_U]:
        return _agroup_map(grouping_function, stream, mapping_functions)

    setattr(_partial, "__stream_map__", _partial)  # See: StreamMapper protocol
    # todo - use f instead?
    return cast(StreamMappable[_T, _U], _partial)


if __name__ == "__main__":

    def mod_three(x: int) -> int:
        return x % 3

    def is_mod_three(x: int) -> bool:
        return x % 3 == 0

    def to_streeeng(x: int) -> str:
        return str(x)

    def to_streeeeeeeeeeng(x: int) -> str:
        return str(x) * 10

    async def main() -> None:
        # s = agroup_map(
        #     mod_three,
        #     arange(100),
        #     {0: to_streeeng, 1: to_streeeeeeeeeeng, Default: to_streeeng},
        # )
        #
        # async for item in s:
        #     print(item)
        #     # reveal_type(item)
        #
        # s_verify = apredicate_map(
        #     arange(100),
        #     {
        #         (lambda x: x % 3): (lambda x: f"boo! divisor! {x}"),
        #         (lambda x: x % 3 == 1): to_streeeng,
        #         Default: (lambda x: f"{x} is k."),
        #     },
        # )

        async def rev_strin(x: str) -> str:
            return x[::-1]

        def first_character(x: str) -> str:
            return x[0]

        def say_dot(x: str) -> str:
            return f"dot! {x}"

        def say_dah(x: str) -> str:
            return f"dah! {x}"

        s_verify = (
            await (
                    amerge(arange_delayed(100), arange_delayed(100, 200))
                    / apredicate_multi_map(
                    {
                        is_mod_three: to_streeeng,
                        Default: to_streeeeeeeeeeng,
                    },
                )
                    / rev_strin
                    / agroup_map(
                    first_character,
                    {
                        ".": say_dot,
                        Default: say_dah,
                    },
                )
                    / F(str.split)(sep="1")
            )
            .aflatmap(lambda x: x)
            .amap(print)
            .run()
        )

        # async for item in s_verify:
        #     print(item)
        # async for itt in s_verify:
        #     print(itt)
        #     # reveal_type(itt)

    asyncio.run(main())

__all__ = (
    "StreamGrouper",
    "agroup_map",
    "apredicate_multi_map",
    "apredicate_map",
)
