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


def run_in_worker_pool(
    n_workers: int,
    queue_size: int = 100,
    start: bool = True,
) -> Callable[[Callable[_P, Coro[_OutputT]]], WorkerPool[_P, _OutputT]]:
    """Decorator to create a worker pool from a function."""
    wp_factory = cast(Callable[_P, WorkerPool[_P, _OutputT]], WorkerPool)  # type: ignore
    return functools.partial(wp_factory, n_workers=n_workers, queue_size=queue_size, start=start)


"""

crazy ideas - ignore

stream = Stream(range(10)) 
    / mul(10) 
    / apply_conditional(on=last_digit, fn={
        0: mul(2),
        1: mul(3) / sum_5,
        5: mul(4),
        DEFAULT: mul(5),
    })
    / div_2
    
stream = Stream(range(10))
    / mul(10)
    / route_conditional(on=last_digit, to={
        0: queue_1 / mul(2) @ sum,
        1: queue_2 / mul(3),
        DEFAULT: PASS_THROUGH,
    )
    / process_passed_through
    
    stream_0s = queue_1 / sum_5
    stream_1s = queue_2 / sum_5
    
stream = Stream(range(10))
    / mul(10)
    / {
        is_fizzbuzz: print(X, "fizzbuzz"),
        is_fizz: print(X, "fizz"),
        is_buzz: print(X, "buzz"),
        DEFAULT: print(X),
    }
    
    
stream = Stream(range(10))
    / mul(10)
    / {
        is_fizzbuzz: Stream / mul_2 / lambda it: print(it, "fizzbuzz"),
        is_fizz: print(X, "fizz"),
        is_buzz: print(X, "buzz"),
        DEFAULT: print(X),
    }

"""

if __name__ == "__main__":

    async def main() -> None:
        @run_in_worker_pool(4)
        async def my_fn(x: int) -> float:
            await asyncio.sleep(0.05)
            return 420 / (x - 69)

        async def funner(range_start: int) -> None:
            for x in range(range_start, range_start + 10):
                f = my_fn(x)
                print(my_fn.n_idle_workers)
                await asyncio.sleep(random.uniform(0.01, 0.1))
                try:
                    print(f"result: {await f}")
                except Exception as e:
                    print(f"error: {e}")
            print("done with", range_start)

        tasks = [asyncio.create_task(funner(i * 10)) for i in range(10)]
        print("waiting for tasks")
        await asyncio.gather(*tasks)
        await my_fn.close()
        print("done")

    asyncio.run(main())
