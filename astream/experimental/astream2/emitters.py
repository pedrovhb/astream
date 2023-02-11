from __future__ import annotations

import asyncio
from typing import TypeVar, AsyncIterable, AsyncIterator, Type
from asynkets import Fuse, EventfulCounter

from astream import Stream
from astream.streamtools import aenumerate

_T = TypeVar("_T")


class Emitter(Stream[_T]):
    def __init__(self) -> None:
        super().__init__(self)
        self._closed = Fuse()
        self._next_item: asyncio.Future[_T] = asyncio.get_running_loop().create_future()
        self._closed_task = asyncio.create_task(self._closed.wait())

    async def __anext__(self) -> _T:
        if self._closed.is_set():
            raise StopAsyncIteration
        done, pending = await asyncio.wait(
            [self._next_item, self._closed_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if self._next_item in done:
            item = self._next_item.result()
            self._next_item = asyncio.get_running_loop().create_future()
            return item
        raise StopAsyncIteration

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    def emit(self, item: _T) -> None:
        self._next_item.set_result(item)

    def close(self) -> None:
        self._closed.set()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()


async def _main() -> None:
    print("emitter created")

    em = Emitter[int]()

    async def _inner() -> None:
        for i in range(10):
            em.emit(i)
            await asyncio.sleep(0.1)
        em.close()

    asyncio.create_task(_inner())
    async for i in em / aenumerate(4):
        print(i)


asyncio.run(_main())

exit(0)


class ConcurrentRunner(Emitter[_T]):
    def __init__(self, max_concurrent: int | None) -> None:
        super().__init__()
        self._max_concurrent = max_concurrent
        self._ongoing = EventfulCounter(initial_value=0, max_value=max_concurrent, min_value=0)

    async def join(self) -> None:
        await self._ongoing.wait_min()

    def _on_task_finished(self, task: asyncio.Task[_T]) -> None:
        self._ongoing -= 1
        try:
            self.emit(task.result())
        except StopAsyncIteration:
            asyncio.create_task(self.close())

    async def close(self) -> None:
        await self.join()
        super().close()

    @classmethod
    def from_stream(
        cls,
        stream: AsyncIterator[_T],
        max_concurrent: int | None,
    ) -> ConcurrentRunner[_T]:
        self = cls(max_concurrent)

        async def _run() -> None:
            async for item in stream:
                await self._ongoing.wait_max_clear()
                self._ongoing += 1
                task = asyncio.create_task(item)
                task.add_done_callback(self._on_task_finished)

            await self.close()

        asyncio.create_task(_run())
        return self

    def __rtruediv__(self, other: AsyncIterable[_T]) -> AsyncIterator[_T]:
        return self.from_stream(aiter(other), self._max_concurrent)

    # def __truediv__(self, other: object) -> AsyncIterator[object]:
    #     return Stream(self) / other


def merge_async_iterables(*async_iters: AsyncIterable[_T]) -> Stream[_T]:
    """Merge multiple async iterables into a single async iterable."""

    async def _inner() -> AsyncIterator[_T]:
        futs: dict[asyncio.Future[_T], AsyncIterator[_T]] = {}
        for it in async_iters:
            async_it = aiter(it)
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

    return Stream(_inner())


if __name__ == "__main__":

    async def demo_concurrent_runner() -> None:
        from astream import Stream

        async def arange_delayed(start: int, stop: int, delay: float) -> AsyncIterator[int]:
            for i in range(start, stop):
                await asyncio.sleep(delay)
                yield i

        stream = Stream(arange_delayed(start=0, stop=20, delay=0.1))
        stream2 = Stream(arange_delayed(start=0, stop=20, delay=0.2))

        merged = merge_async_iterables(stream, stream2)

        async def work(n: int) -> int:
            await asyncio.sleep(1)
            return n**2

        async for i, item in merged / (lambda it: work(it)) / ConcurrentRunner(3) / aenumerate:
            print(i, item)

    asyncio.run(demo_concurrent_runner())


__all__ = ("ConcurrentRunner", "Emitter", "merge_async_iterables")
