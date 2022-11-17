from __future__ import annotations

import asyncio
from asyncio import Future
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Coroutine,
    Generic,
    Iterable,
    ParamSpec,
    TypeAlias,
    TypeVar,
    cast,
    overload,
)

from astream import (
    Stream,
    StreamFilterable,
    StreamFlatMappable,
    StreamMappable,
    afilter,
    aflatmap,
    amap,
    ensure_coro_fn,
)
from astream.closeable_queue import CloseableQueue

_T = TypeVar("_T")
_U = TypeVar("_U")
_R = TypeVar("_R")
_P = ParamSpec("_P")


_CoroT: TypeAlias = Coroutine[Any, Any, _T]


class WorkerPool(
    StreamMappable[_T, _R], StreamFilterable[_T], StreamFlatMappable[_T, _R], Generic[_T, _R]
):
    @overload
    def __init__(self, fn: Callable[[_T], _CoroT[_R]], num_workers: int = ...) -> None:
        ...

    @overload
    def __init__(self, fn: Callable[[_T], _R], num_workers: int = ...) -> None:
        ...

    def __init__(
        self, fn: Callable[[_T], _CoroT[_R]] | Callable[[_T], _R], num_workers: int = 5
    ) -> None:
        self._fn = cast(Callable[[_T], _CoroT[_R]], ensure_coro_fn(fn))
        self._num_workers = num_workers

        self.in_q = CloseableQueue[_T](self._num_workers)
        self.out_q = CloseableQueue[_R](self._num_workers)

    async def _feed(self, stream: AsyncIterator[_T]) -> None:
        async for item in stream:
            await self.in_q.put(item)
        self.in_q.close()

    def _stream_op(self, stream: Stream[_T], op: Callable[..., Stream[_R]]) -> Stream[_R]:

        worker_done_futs = tuple(
            asyncio.get_running_loop().create_future() for _ in range(self._num_workers)
        )

        async def _worker(done_fut: Future[None]) -> None:
            async for item in op(self._fn, self.in_q):
                await self.out_q.put(item)
            done_fut.set_result(None)
            await self.in_q.wait_exhausted()
            await asyncio.gather(*worker_done_futs)
            self.out_q.close()

        asyncio.create_task(self._feed(stream))
        for fut in worker_done_futs:
            asyncio.create_task(_worker(fut))

        return Stream(self.out_q)

    def __stream_map__(self, stream: Stream[_T]) -> Stream[_R]:
        return self._stream_op(stream, amap)

    def __stream_filter__(self: WorkerPool[_T, _T], stream: Stream[_T]) -> Stream[_T]:
        return self._stream_op(stream, afilter)

    @overload
    def __stream_flatmap__(self, stream: Stream[AsyncIterable[_U]]) -> Stream[_R]:
        ...

    @overload
    def __stream_flatmap__(self, stream: Stream[Iterable[_U]]) -> Stream[_R]:
        ...

    def __stream_flatmap__(
        self, stream: Stream[AsyncIterable[_U]] | Stream[Iterable[_U]]
    ) -> Stream[_R]:
        ...
        return self._stream_op(stream, aflatmap)


__all__ = ("WorkerPool",)

if __name__ == "__main__":

    async def main() -> None:
        async def fn(i: int) -> int:
            await asyncio.sleep(0.01)
            return i

        async def fnfilter(i: int) -> bool:
            await asyncio.sleep(1)
            return i % 7 == 0

        st = (
            Stream(range(100))
            / WorkerPool(fn, 10)
            % WorkerPool(fnfilter, 10)
            // WorkerPool(range, 10)
        )

        async for item in st:
            print(item)

    asyncio.run(main())

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

# if __name__ == "__main__":
#
#     async def main() -> None:
#         async def my_fn(x: int) -> float:
#             await asyncio.sleep(0.05)
#             return 420 / (x - 69)
#
#         async def funner(range_start: int) -> None:
#             for x in range(range_start, range_start + 10):
#                 f = my_fn(x)
#                 print(my_fn.n_idle_workers)
#                 await asyncio.sleep(random.uniform(0.01, 0.1))
#                 try:
#                     print(f"result: {await f}")
#                 except Exception as e:
#                     print(f"error: {e}")
#             print("done with", range_start)
#
#         tasks = [asyncio.create_task(funner(i * 10)) for i in range(10)]
#         print("waiting for tasks")
#         await asyncio.gather(*tasks)
#         await my_fn.close()
#         print("done")
#
#     asyncio.run(main())
