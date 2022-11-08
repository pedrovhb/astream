import asyncio
import os
import pickle
import random
import math
import time
from asyncio import CancelledError
from queue import Queue, Full
from typing import AsyncIterator, Iterable, TypeVar, Iterator

from astream import aenumerate


def approximate_pi() -> Iterator[float]:
    print("Starting")
    total_points = 0
    within_circle = 0
    while True:
        x = random.random()
        y = random.random()
        total_points += 1
        distance = math.sqrt(x**2 + y**2)
        if distance < 1:
            within_circle += 1
        estimate = 4.0 * within_circle / total_points
        if total_points % 100 == 0:
            print(total_points)
        yield estimate


T = TypeVar("T")


async def _iter_to_aiter_threaded(
    iterable: Iterable[T],
    queue_max_size: int = 10_000,
    high_sleep: float = 0.1,
) -> AsyncIterator[T]:
    """Convert an iterable to an async iterable (running the iterable in a background thread)."""

    # os.mkfifo("fifo")

    def _inner() -> None:
        fr = os.open("fifo", os.O_WRONLY)

        print(f"inner started")
        last_put = time.perf_counter()
        to_put_time = 0.0
        with open("fifo", "wb") as fd_w:
            for it in iterable:
                pickle.dump(it, fd_w)
            # os.write(fr,  it)//
            # try:
            #     time.sleep(0.00001)
            #     q.put_nowait(it)
            # except (TimeoutError, Full):
            #     print(f"full (t:    {to_put_time:.10f})")
            #     t = time.perf_counter()
            #     q.put(it)
            #     to_put_time = time.perf_counter() - t
            #     to_put_time *= 2
            #     print(f"new to_put: {to_put_time}")

            # if q.qsize() > high:
            #     print("high")
            #     time.sleep(0.1)
        # print("inner done")
        # queue_done.set_result(True)

    thread_task = asyncio.create_task(asyncio.to_thread(_inner))
    try:
        with open("fifo", "rb") as fd:
            while True:
                i = pickle.load(fd)
                print(i)
                # while not (queue_done.done() and q.empty()):
                #     q.
                yield i
    except CancelledError:
        thread_task.cancel()
        raise


async def main():

    api = _iter_to_aiter_threaded(approximate_pi())

    # prev_total = total_points
    # async def print_stats():
    #     nonlocal prev_total
    #     while True:
    #         prev_total = total_points
    #         prev_dt = datetime.now()
    #         await asyncio.sleep(1)
    #         t = datetime.now() - prev_dt
    #         print(
    #             f"Total points: {total_points}, within circle: {within_circle}, "
    #             f"pi estimate: {estimate}, per second: {(total_points - prev_total)/t.total_seconds()} (loop: {t})"
    #         )
    # asyncio.create_task(print_stats())

    async for i, val in aenumerate(api):
        if i % 10000 == 0:
            print(i)


if __name__ == "__main__":
    asyncio.run(main())
