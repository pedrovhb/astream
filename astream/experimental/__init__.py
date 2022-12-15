from __future__ import annotations

import asyncio
import sys
from asyncio import Future
from functools import cached_property
from types import TracebackType
from typing import (
    TypeVar,
    Any,
    Generic,
    overload,
    Callable,
    TYPE_CHECKING,
    AsyncGenerator,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Type,
    Generator,
    cast,
)

from abc import ABC, abstractmethod
from typing import TypeVar, Any

from loguru import logger

from astream.closeable_queue import CloseableQueue

T = TypeVar("T")
# logger.add(print, level="DEBUG")
logger.debug("hello")


class BaseOperatorExtensible(Generic[T]):
    def __init__(self) -> None:
        pass

    @overload
    def __truediv__(self, other: StreamTransformer[T, R]) -> R:
        ...

    @overload
    def __truediv__(self, other: Any) -> Any:
        ...

    def __truediv__(self, other: Any) -> Any:
        if hasattr(other, "__on_truediv__"):
            return other.__on_truediv__(self)
        return self / other

    @overload
    def __rtruediv__(self, other: StreamTransformer[T, R]) -> R:
        ...

    @overload
    def __rtruediv__(self, other: Any) -> Any:
        ...

    def __rtruediv__(self, other: Any) -> Any:
        if hasattr(other, "__on_rtruediv__"):
            return other.__on_rtruediv__(self)
        return other / self

    @overload
    def __floordiv__(self, other: StreamTransformer[T, R]) -> R:
        ...

    @overload
    def __floordiv__(self, other: Any) -> Any:
        ...

    def __floordiv__(self, other: Any) -> Any:
        if hasattr(other, "__on_floordiv__"):
            return other.__on_floordiv__(self)
        return self // other

    @overload
    def __rfloordiv__(self, other: StreamTransformer[T, R]) -> R:
        ...

    @overload
    def __rfloordiv__(self, other: Any) -> Any:
        ...

    def __rfloordiv__(self, other: Any) -> Any:
        if hasattr(other, "__on_rfloordiv__"):
            return other.__on_rfloordiv__(self)
        return other // self

    @overload
    def __mod__(self, other: StreamTransformer[T, R]) -> R:
        ...

    @overload
    def __mod__(self, other: Any) -> Any:
        ...

    def __mod__(self, other: Any) -> Any:
        if hasattr(other, "__on_mod__"):
            return other.__on_mod__(self)
        return self % other

    @overload
    def __rmod__(self, other: StreamTransformer[T, R]) -> R:
        ...

    @overload
    def __rmod__(self, other: Any) -> Any:
        ...

    def __rmod__(self, other: Any) -> Any:
        if hasattr(other, "__on_rmod__"):
            return other.__on_rmod__(self)
        return other % self


R = TypeVar("R")


class StreamTransformer(ABC, Generic[T, R]):
    @abstractmethod
    def __on_truediv__(self, other: T) -> Stream[R]:
        pass

    @abstractmethod
    def __on_rtruediv__(self, other: T) -> Stream[R]:
        pass


class StreamMapper(Generic[T, R], StreamTransformer[T, R]):
    def __init__(self, fn: Callable[[T], R]):
        def _fn(x: T | Stream[T]) -> Stream[R]:
            if isinstance(x, Stream):
                return Stream(fn(x._value))
            return Stream(fn(x))

        self._fn = fn

    def __on_truediv__(self, other: Stream[T]) -> R:
        return self._fn(other)

    def __on_rtruediv__(self, other: Stream[T]) -> R:
        return self._fn(other)


YieldType = TypeVar("YieldType", covariant=True)
SendType = TypeVar("SendType", contravariant=True)
ResultType = TypeVar("ResultType")

_BaseStreamT = TypeVar("_BaseStreamT", bound="_BaseStream[Any, Any]")


class _BaseStream(
    Generic[YieldType, SendType],
    AsyncGenerator[YieldType, SendType],
    ABC,
):
    def __init__(self, *args: object, **kwargs: object) -> None:
        logger.debug(f"[Stream.__init__()     ] - {self!r}")
        super().__init__()

        self._closed = asyncio.Event()
        self._result: Future[YieldType] = asyncio.get_event_loop().create_future()
        self._latest_value: YieldType | None = None

        self._get_result_task: asyncio.Task[YieldType] = asyncio.create_task(self._get_result())

    @classmethod
    def _wrap_asend(
        cls: Type[_BaseStreamT],
    ) -> Callable[[_BaseStreamT, SendType], Awaitable[YieldType]]:
        _cls = cast(Type[_BaseStream[YieldType, SendType]], cls)
        original_asend = _cls.asend

        async def _asend(self: _BaseStream[YieldType, SendType], value: SendType) -> YieldType:
            if self._closed.is_set():
                raise StopAsyncIteration
            result = await original_asend(self, value)
            self._latest_value = result
            return result

        return _asend

    def _get_result(self) -> Generator[Any, None, Future[YieldType]]:
        logger.warning(f"[Stream._get_result()  ] - _get_result started {self!r}")
        task = asyncio.create_task(self._closed.wait())
        yield from task.__await__()

        if self._latest_value is not None:
            logger.debug(f"[Stream._get_result()  ] - _closed done {self!r}")
            self._result.set_result(self._latest_value)
            return self._result
        else:
            logger.debug(f"[Stream._get_result()  ] - _closed done, but no values {self!r}")
            self._result.set_exception(StopAsyncIteration)
            return self._result

    def __init_subclass__(cls: Type[_BaseStream[YieldType, SendType]], **kwargs: object) -> None:
        logger.debug(f"[Stream.__init_subclass__()] - {cls}, {kwargs}")
        cls.asend = cls._wrap_asend()  # type: ignore
        super().__init_subclass__(**kwargs)

    @overload
    def athrow(
        self,
        __typ: Type[BaseException],
        __val: BaseException | object = ...,
        __tb: TracebackType | None = ...,
    ) -> Awaitable[YieldType]:
        ...

    @overload
    def athrow(
        self, __typ: BaseException, __val: None = ..., __tb: TracebackType | None = ...
    ) -> Awaitable[YieldType]:
        ...

    def athrow(
        self,
        __typ: Type[BaseException] | BaseException,
        __val: BaseException | object | None = None,
        __tb: TracebackType | None = None,
    ) -> Awaitable[YieldType]:
        logger.exception(
            f"[Stream.athrow()      ] - {__typ=!r}, {__val=!r}, {__tb=!r}, {self=!r}",
            exc_info=__val,
        )
        self._closed.set()
        if isinstance(__typ, BaseException):
            return super(_BaseStream, self).athrow(__typ)
        else:
            return super(_BaseStream, self).athrow(__typ, __val, __tb)

    async def aclose(self) -> None:
        logger.debug(f"[Stream.aclose()      ] - {self!r}")
        return await super().aclose()

    @abstractmethod
    def asend(self, __val: SendType) -> Awaitable[YieldType]:
        ...

    # async def _anext_template(self, to_send: SendType) -> YieldType:
    #     logger.debug(f"[Stream._anext_template()] - {self!r}")
    #
    #     if self._closed.is_set():
    #         logger.debug(f"[Stream._anext_template()] - _closed is set {self!r}")
    #         raise StopAsyncIteration
    #
    #     task_closed = asyncio.create_task(self._closed.wait())
    #     task_asend = asyncio.ensure_future(self.asend(to_send))
    #     done, pending = await asyncio.wait(
    #         {task_closed, task_asend},
    #         return_when=asyncio.FIRST_COMPLETED,
    #     )
    #     if task_asend in done:
    #         return await task_asend
    #     else:
    #         logger.debug(f"[Stream._anext_template()] - _closed done {self!r}")
    #         raise StopAsyncIteration

    async def wait_get_last(self) -> YieldType:
        """Don't consume the stream, just await for it to be done. Return the last value."""
        return await self._result

    async def consume_get_last(self) -> YieldType:
        """Consume the stream and return the last value."""
        async for _ in self:
            pass
        return await self._result

    async def close_get_last(self) -> YieldType:
        """Close the stream and return the last value yielded."""
        await self.aclose()
        return await self._result

    def __await__(self) -> Generator[Any, None, YieldType]:
        logger.debug(f"[Stream.__await__()   ] - {self!r}")

        yield from self.consume_get_last().__await__()
        return self._result.result()

    async def __anext__(self) -> YieldType:
        logger.debug(f"[Stream.__anext__()   ] - {self!r}")
        return await self.asend(None)  # todo: this is a hack

    def __aiter__(self) -> _BaseStream[YieldType, SendType]:
        logger.debug(f"[Stream.__aiter__()   ] - {self!r}")
        return self

    def __repr__(self) -> str:
        return f"<Stream {self.__class__.__name__}>"


class AsyncIterableStream(_BaseStream[T, None]):
    def __init__(self, async_iterable: AsyncIterable[T]) -> None:
        super().__init__()
        self._agen = self._get_agen(aiter(async_iterable))

    async def _process(self, __value: SendType) -> T:
        logger.debug(f"Stream._process({__value})")
        return await self._agen.asend(None)

    async def _get_agen(self, async_iterator: AsyncIterator[T]) -> AsyncGenerator[T, None]:
        async for value in async_iterator:
            yield value

    async def asend(self, value: None) -> T:
        try:
            return await self._process(value)
        except StopAsyncIteration:
            self._closed.set()
            raise


class FilteringStream(_BaseStream[T, None]):
    def __init__(self, async_iterable: AsyncIterable[T], filter_fn: Callable[[T], bool]) -> None:
        super().__init__()
        self._agen = self._get_agen(aiter(async_iterable))
        self._filter_fn = filter_fn

    async def _process(self, __value: None) -> T:
        item = await self._agen.asend(None)
        while not self._filter_fn(item):
            item = await self._agen.asend(None)
        return item

    async def _get_agen(self, async_iterator: AsyncIterator[T]) -> AsyncGenerator[T, None]:
        async for value in async_iterator:
            yield value

    async def asend(self, value: None) -> T:
        try:
            return await self._process(value)
        except StopAsyncIteration:
            self._closed.set()
            raise


if not TYPE_CHECKING:

    def reveal_type(obj: Any) -> None:
        print(type(obj))


if __name__ == "__main__":

    async def test_await_modes() -> None:
        for fn in (
            _BaseStream.consume_get_last,
            _BaseStream.close_get_last,
            _BaseStream.wait_get_last,
        ):
            print(fn.__name__)

            strem = AsyncIterableStream(myarange(1, 10))
            async for i in strem:
                print(i)
                if i == 5:
                    break

            print(fn, await fn(strem))

    async def test_await() -> None:
        strem = AsyncIterableStream(myarange(1, 10))
        async for i in strem:
            print(i)
            if i == 5:
                break

        print(await strem)

    async def test_afilter() -> None:
        strem = FilteringStream(myarange(1, 10), lambda x: x % 2 == 0)
        async for i in strem:
            print(i)

        print(await strem)

    def mul_two(x: int) -> int:
        return x * 2

    def to_str(x: int) -> str:
        return str(x)

    async def myarange(start: int, stop: int) -> AsyncIterable[int]:
        for i in range(start, stop):
            yield i

    # Set loguru level to DEBUG
    # logger.remove()

    async def main() -> None:

        # ar = myarange(10, 20)
        # async for i in ar:
        #     print("1", i)
        #
        # async for i in ar:
        #     print("2", i)

        await test_await()
        await test_afilter()
        await asyncio.sleep(0.3)

    asyncio.run(main(), debug=True)
    # result = stream / StreamMapper(mul_two)
    # reveal_type(result)
    # print(result)
