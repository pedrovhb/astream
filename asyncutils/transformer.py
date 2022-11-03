from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import AsyncIterator, AsyncIterable, Callable, Iterable, Iterator, Coroutine
from itertools import pairwise
from types import NotImplementedType
from typing import TypeVar, Generic, cast, Any, Type, Protocol, overload, Awaitable

from asyncutils.afilter import iter_to_aiter
from asyncutils.amerge import amerge
from asyncutils.closeable_queue import CloseableQueue
from asyncutils.misc import arange
from asyncutils.protocols import CloseableQueueLike

_InputT = TypeVar("_InputT")
_OutputT = TypeVar("_OutputT")
_NextT = TypeVar("_NextT")
_T = TypeVar("_T")


async def transformer_noop(item: _InputT) -> AsyncIterator[_OutputT]:
    yield cast(_OutputT, item)


class Stream(Generic[_OutputT], AsyncIterable[_OutputT]):
    def __init__(
        self,
        iterable: AsyncIterable[_OutputT],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> None:
        self._out_q = CloseableQueue[_OutputT](maxsize=max_out_q_size)

        self._started = asyncio.Event()
        self._closed = asyncio.Event()

        if isinstance(iterable, AsyncIterable):
            async_iterable = iterable
        # elif isinstance(iterable, Iterable):
        #     async_iterable = iter_to_aiter(iterable)  # todo - doesn't seem to be working
        else:
            raise TypeError(f"StreamTransformer got non-iterable and non-asynciterable {iterable}")
        self._async_iterable = async_iterable

        if start:
            self.start()

    def start(self) -> None:
        if self._started.is_set():
            raise RuntimeError("Already started.")
        self._started.set()
        asyncio.create_task(self._feed(self._async_iterable))

    async def _feed(self, async_iterable: AsyncIterable[_InputT]) -> None:
        async for item in async_iterable:
            await self._out_q.put(item)
        self._out_q.close()

    async def wait_started(self) -> None:
        await self._started.wait()

    async def wait_exhausted(self) -> None:
        await self._out_q.wait_exhausted()

    async def wait_closed(self) -> None:
        await self._out_q.wait_closed()

    async def gather(self) -> list[_OutputT]:
        return [it async for it in self]

    def __aiter__(self) -> AsyncIterator[_OutputT]:
        return aiter(self._out_q)

    @classmethod
    @overload
    def from_filter(
        cls,
        async_iterable: AsyncIterable[_T],
        filter_func: Callable[[_T], Coroutine[Any, Any, bool]],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_T]:
        ...

    @classmethod
    @overload
    def from_filter(
        cls,
        async_iterable: AsyncIterable[_T],
        filter_func: Callable[[_T], bool],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_T]:
        ...

    @classmethod
    def from_filter(
        cls,
        async_iterable: AsyncIterable[_T],
        filter_func: Callable[[_T], Any],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_T]:

        if inspect.iscoroutinefunction(filter_func):
            _ff_async = filter_func

            @functools.wraps(filter_func)
            async def _as_filter(item: _T) -> AsyncIterator[_T]:
                if await _ff_async(item):
                    yield item

        elif callable(filter_func):
            _ff_sync = filter_func

            @functools.wraps(filter_func)
            async def _as_filter(item: _T) -> AsyncIterator[_T]:
                if _ff_sync(item):
                    yield item

        else:
            raise RuntimeError(f"Function {filter_func} is not callable.")
        cls_ = cast(Type[Stream[_T]], cls)
        return cls_(
            async_iterable,
            _as_filter,
            max_out_q_size,
            start,
        )

    @classmethod
    @overload
    def from_map(
        cls,
        async_iterable: AsyncIterable[_InputT],
        map_func: Callable[[_InputT], Coroutine[Any, Any, _OutputT]]
        | Callable[[_InputT], _OutputT],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_OutputT]:
        ...

    @classmethod
    @overload
    def from_map(
        cls,
        async_iterable: AsyncIterable[_InputT],
        map_func: Callable[[_InputT], _OutputT] | Callable[[_InputT], _OutputT],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_OutputT]:
        ...

    @classmethod
    def from_map(
        cls,
        async_iterable: AsyncIterable[_InputT],
        map_func: Callable[[_InputT], Any],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_OutputT]:

        if inspect.iscoroutinefunction(map_func):
            _mf_async = map_func

            @functools.wraps(map_func)
            async def _as_map(item: _InputT) -> AsyncIterator[_OutputT]:
                yield await _mf_async(item)

        elif callable(map_func):
            _mf_sync = cast(Callable[[_InputT], _OutputT], map_func)

            @functools.wraps(map_func)
            async def _as_map(item: _InputT) -> AsyncIterator[_OutputT]:
                yield _mf_sync(item)

        else:
            raise RuntimeError(f"Function {map_func} is not callable.")

        return cls(
            async_iterable,
            _as_map,
            max_out_q_size,
            start,
        )

    @classmethod
    @overload
    def from_flatmap(
        cls,
        async_iterable: AsyncIterable[_InputT],
        flatmap_func: Callable[[_InputT], Coroutine[Any, Any, Iterable[_OutputT]]],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_OutputT]:
        ...

    @classmethod
    @overload
    def from_flatmap(
        cls,
        async_iterable: AsyncIterable[_InputT],
        flatmap_func: Callable[[_InputT], Iterable[_OutputT]],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_OutputT]:
        ...

    @classmethod
    @overload
    def from_flatmap(
        cls,
        async_iterable: AsyncIterable[_InputT],
        flatmap_func: Callable[[_InputT], AsyncIterable[_OutputT]],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_OutputT]:
        ...

    @classmethod
    def from_flatmap(
        cls,
        async_iterable: AsyncIterable[_InputT],
        flatmap_func: Callable[[_InputT], Coroutine[Any, Any, Iterable[_OutputT]]]
        | Callable[[_InputT], Iterable[_OutputT]]
        | Callable[[_InputT], AsyncIterable[_OutputT]],
        max_out_q_size: int = 0,
        start: bool = True,
    ) -> Stream[_OutputT]:

        if inspect.iscoroutinefunction(flatmap_func):
            _mf_async = flatmap_func

            @functools.wraps(flatmap_func)
            async def _as_flatmap(item: _InputT) -> AsyncIterator[_OutputT]:
                for transformed in await _mf_async(item):
                    yield transformed

        elif inspect.isasyncgenfunction(flatmap_func):
            _mf_async_gen = cast(Callable[[_InputT], AsyncIterable[_OutputT]], flatmap_func)

            @functools.wraps(flatmap_func)
            async def _as_flatmap(item: _InputT) -> AsyncIterator[_OutputT]:
                async for transformed in _mf_async_gen(item):
                    yield transformed

        elif callable(flatmap_func):
            _mf_sync = cast(Callable[[_InputT], Iterable[_OutputT]], flatmap_func)

            @functools.wraps(flatmap_func)
            async def _as_flatmap(item: _InputT) -> AsyncIterator[_OutputT]:
                for transformed in _mf_sync(item):
                    yield transformed

        else:
            raise RuntimeError(f"Function {flatmap_func} is not callable.")

        return cls(
            async_iterable,
            _as_flatmap,
            max_out_q_size,
            start,
        )

    @overload
    def amap(self, other: Callable[[_OutputT], Coroutine[Any, Any, _NextT]]) -> Stream[_NextT]:
        ...

    @overload
    def amap(self, other: Callable[[_OutputT], _NextT]) -> Stream[_NextT]:
        ...

    def amap(self, other: Any) -> Stream[_NextT]:
        cls = self.__class__
        ret = cls.from_map(self, other, self._out_q.maxsize, self._started.is_set())
        return cast(Stream[_NextT], ret)

    @overload
    def aflatmap(
        self, other: Callable[[_OutputT], Coroutine[Any, Any, Iterable[_NextT]]]
    ) -> Stream[_NextT]:
        ...

    @overload
    def aflatmap(self, other: Callable[[_OutputT], Iterable[_NextT]]) -> Stream[_NextT]:
        ...

    @overload
    def aflatmap(self, other: Callable[[_OutputT], AsyncIterable[_NextT]]) -> Stream[_NextT]:
        ...

    def aflatmap(self, other: Any) -> Stream[_NextT]:
        cls = self.__class__
        ret = cls.from_flatmap(self, other, self._out_q.maxsize, self._started.is_set())
        return cast(Stream[_NextT], ret)

    @overload
    def afilter(self, other: Callable[[_OutputT], Coroutine[Any, Any, bool]]) -> Stream[_OutputT]:
        ...

    @overload
    def afilter(self, other: Callable[[_OutputT], bool]) -> Stream[_OutputT]:
        ...

    def afilter(self, other: Any) -> Stream[_OutputT]:
        cls = self.__class__
        return cls.from_filter(self, other, self._out_q.maxsize, self._started.is_set())

    @overload
    def agather(
        self, fn: Callable[[list[_OutputT]], Coroutine[Any, Any, _NextT]]
    ) -> Awaitable[_NextT]:
        ...

    @overload
    def agather(self, fn: Callable[[list[_OutputT]], _NextT]) -> Awaitable[_NextT]:
        ...

    def agather(self, fn: Any) -> Awaitable[_NextT]:
        async def _coro() -> _NextT:
            if inspect.iscoroutinefunction(fn):
                _fn_async = fn
                ret = await _fn_async([item async for item in self])
            elif callable(fn):
                _fn_sync = fn
                ret = _fn_sync([item async for item in self])
            else:
                raise TypeError(f"Cannot gather non-callable {fn}")
            return cast(_NextT, ret)

        return _coro()

    __truediv__ = amap
    __floordiv__ = aflatmap
    __mod__ = afilter
    __matmul__ = agather
