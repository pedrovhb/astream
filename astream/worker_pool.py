from __future__ import annotations

import asyncio
import functools
import inspect
import random
from asyncio import Future
from typing import (
    Any,
    Callable,
    cast,
    Coroutine,
    Generator,
    Generic,
    overload,
    ParamSpec,
    TypeAlias,
    TypeVar,
)

from astream.closeable_queue import CloseableQueue

_T = TypeVar("_T")
_P = ParamSpec("_P")
_P2 = ParamSpec("_P2")

_InputT = TypeVar("_InputT")
_OutputT = TypeVar("_OutputT")

Coro: TypeAlias = Coroutine[Any, Any, _OutputT]


class WorkerPool(Generic[_P, _OutputT]):
    _acall: Callable[_P, Coro[_OutputT]]

    @overload
    def __init__(
        self,
        fn: Callable[_P, Coro[_OutputT]],
        n_workers: int,
        queue_size: int = ...,
        start: bool = ...,
    ) -> None:
        ...

    @overload
    def __init__(
        self,
        fn: Callable[_P, _OutputT],
        n_workers: int,
        queue_size: int = ...,
        start: bool = ...,
    ) -> None:
        ...

    def __init__(
        self,
        fn: Callable[_P, Coro[_OutputT]] | Callable[_P, _OutputT],
        n_workers: int,
        queue_size: int = 100,
        start: bool = True,
    ) -> None:

        # todo - add option to either keep items in queue or discard them if not iterated on
        #  (or have separate classes for either)

        if not inspect.iscoroutinefunction(fn):
            _fn_sync = cast(Callable[_P, _OutputT], fn)

            async def _fn(*args: _P.args, **kwargs: _P.kwargs) -> _OutputT:
                return await asyncio.to_thread(_fn_sync, *args, **kwargs)

        else:
            _fn = cast(Callable[_P, Coro[_OutputT]], fn)

        self._fn = _fn

        self._in_q = CloseableQueue[tuple[Future[_OutputT], Coro[_OutputT]]](queue_size)

        self._n_workers = n_workers
        self._workers: list[asyncio.Task[None]] = []
        self._idle_worker_events = [asyncio.Event() for _ in range(self._n_workers)]

        @functools.wraps(fn)
        async def _acall(*args: _P.args, **kwargs: _P.kwargs) -> _OutputT:
            if self._in_q.is_closed:
                raise RuntimeError("Pool is closed")
            fut: Future[_OutputT] = asyncio.get_running_loop().create_future()
            await self._in_q.put((fut, self._fn(*args, **kwargs)))
            item = await fut
            return item

        self._acall = _acall

        self._running = asyncio.Event()
        if start:
            self.start()

    def start(self) -> None:
        self._workers = [asyncio.create_task(self._worker(ev)) for ev in self._idle_worker_events]
        self._running.set()

    async def resume(self) -> None:
        self._running.set()

    async def pause(self) -> None:
        self._running.clear()

    async def close(self) -> None:
        self._in_q.close()
        await self._in_q.wait_exhausted()

    @property
    def n_idle_workers(self) -> int:
        return sum(ev.is_set() for ev in self._idle_worker_events)

    def __await__(self) -> Generator[Any, None, None]:
        return asyncio.ensure_future(self._in_q.wait_exhausted()).__await__()

    def __call__(self, *args: _P.args, **kwargs: _P.kwargs) -> Coroutine[Any, Any, _OutputT]:
        return self._acall(*args, **kwargs)

    async def _worker(self, worker_idle_event: asyncio.Event) -> None:
        async for fut, coro in self._in_q:
            await self._running.wait()
            worker_idle_event.clear()
            try:
                result = await coro
            except Exception as e:
                fut.set_exception(e)
            else:
                fut.set_result(result)
            finally:
                if self._in_q.empty():
                    worker_idle_event.set()


# class AsyncIterableWorkerPool(Generic[_P2, _OutputT], WorkerPool[_P2, _OutputT]):
#     def __init__(
#         self,
#         fn: Callable[_P2, Coro[_OutputT]] | Callable[_P2, _OutputT],
#         n_workers: int,
#         queue_size: int = 100,
#         start: bool = True,
#     ) -> None:
#         super().__init__(fn, n_workers, queue_size, start)
#
#     def _on_input_exhausted(self, fut: Future[None]) -> None:
#         print("input exhausted, closing output")
#         asyncio.create_task(self._out_q.wait_closed()).add_done_callback(
#             lambda _: print("output closed")
#         )
#         asyncio.create_task(self._out_q.wait_exhausted()).add_done_callback(
#             lambda _: print("output exhausted")
#         )
#         # asyncio.create_task(self._out_q.wait_exhausted()).add_done_callback(
#         #     lambda _: self._async_iter_task.cancel()
#         # )
#         self._out_q.close()
#
#     def close(self) -> None:
#         if not self._in_q.is_closed:
#             self._in_q.close()
#         # todo - how do we properly wait for items to be done?
#         #  what if the stream is never iterated on and the queue is never exhausted/just blocks?
#         #  keep items only if aiter or stream have been accessed?
#         #  have a drainer that just discards items on the async if the pool is created without
#         #    being async iterable?
#
#     @cached_property
#     async def _async_iter(self) -> AsyncIterator[_OutputT]:
#         # todo - how to close this? is it necessary?
#         async for fut, item in self._out_q:
#             yield item
#
#     @cached_property
#     def stream(self) -> Stream[_OutputT]:
#         print("creating stream")
#         return Stream(self._async_iter)
#
#     def __aiter__(self) -> AsyncIterator[_OutputT]:
#         return self.stream


def run_in_worker_pool(
    n_workers: int,
    queue_size: int = 100,
    start: bool = True,
) -> Callable[[Callable[_P, Coro[_OutputT]]], WorkerPool[_P, _OutputT]]:
    """Decorator to create a worker pool from a function."""
    wp_factory = cast(Callable[_P, WorkerPool[_P, _OutputT]], WorkerPool)  # type: ignore
    return functools.partial(wp_factory, n_workers=n_workers, queue_size=queue_size, start=start)


if __name__ == "__main__":

    async def main() -> None:
        @run_in_worker_pool(5, 46)
        async def fn(x: int) -> float:
            await asyncio.sleep(0.05)
            return 420 / (x - 69)

        # todo - there's a bug in arange/Stream that causes it to not stop

        async def funner(range_start: int) -> None:
            for x in range(range_start, range_start + 10):
                f = fn(x)
                print(fn.n_idle_workers)
                await asyncio.sleep(random.uniform(0.01, 0.1))
                try:
                    print(f"result: {await f}")
                except Exception as e:
                    print(f"error: {e}")
            print("done with", range_start)

        tasks = [asyncio.create_task(funner(i * 10)) for i in range(10)]
        print("waiting for tasks")
        await asyncio.gather(*tasks)
        await fn.close()
        print("done")

    asyncio.run(main())
