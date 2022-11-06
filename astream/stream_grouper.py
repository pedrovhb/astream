from __future__ import annotations

import asyncio
import functools
from asyncio import Future, Task
from collections import defaultdict
from typing import *

from astream.closeable_queue import CloseableQueue
from astream.stream import Stream
from astream.utils import create_future, ensure_coro_fn

_T = TypeVar("_T")
_U = TypeVar("_U")
_KeyT = TypeVar("_KeyT")

_P = ParamSpec("_P")
Coro: TypeAlias = Coroutine[Any, Any, _T]
_GroupingFunctionT: TypeAlias = Callable[[_T], _KeyT]


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

    @overload
    def __init__(
        self,
        grouping_function: _GroupingFunctionT[_T, Coro[_KeyT]],
        group_stream: AsyncIterable[_T],
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        grouping_function: _GroupingFunctionT[_T, _KeyT],
        group_stream: AsyncIterable[_T],
    ) -> None:
        ...

    def __init__(
        self,
        grouping_function: _GroupingFunctionT[_T, _KeyT] | _GroupingFunctionT[_T, Coro[_KeyT]],
        group_stream: AsyncIterable[_T],
    ) -> None:
        self._grouping_function = ensure_coro_fn(grouping_function)
        self._source_stream = aiter(group_stream)

        self._key_queues: dict[_KeyT, CloseableQueue[_T]] = {}
        self._key_streams: defaultdict[_KeyT, Future[Stream[_T]]]
        self._key_streams = defaultdict(create_future)

        # self._key_exists: defaultdict[_KeyT, Future[None]]
        # self._key_exists = defaultdict(lambda: asyncio.get_running_loop().create_future())

        self._new_key_queue: CloseableQueue[_KeyT] = CloseableQueue()
        self._akeys_stream = None

        self._grouping_task = None

    # async def _aiter_for_key(self, key: _KeyT) -> AsyncIterator[_T]:
    #     """Returns an async iterator for the given key.
    #
    #     That is, this function returns an async iterator that yields items from the queue
    #     for which the grouping function returned the given key.
    #     """
    #     print("aiter_for_key", key)
    #     if not (key_exists_fut := self._key_exists[key]).done():
    #         await key_exists_fut
    #
    #     print("aiter_for_key", key, "exists")
    #     async for it in self._key_queues[key]:
    #         yield it

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

        # await asyncio.gather(*(q.wait_exhausted() for q in self._key_queues.values()))

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


__all__ = ("StreamGrouper",)
