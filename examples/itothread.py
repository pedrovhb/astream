import asyncio
import math
import os
import pickle
import random
import time
from asyncio import CancelledError
from datetime import datetime
from queue import Full, Queue
from typing import AsyncIterator, Iterable, Iterator, TypeVar

from loguru import logger

from astream import aenumerate, iter_to_aiter


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
        if total_points % 10000 == 0:
            yield estimate
        # time.sleep(0.00001)
        # yield estimate


T = TypeVar("T")


async def main() -> None:

    api = iter_to_aiter(approximate_pi(), to_thread=True)

    total_points, calculated_val = 0, 0.0
    # prev_total = 0

    async def print_stats() -> None:
        print(f"pi = {calculated_val:.12f} (after {total_points} iterations)")
        prev_total = 0
        while True:
            prev_dt = datetime.now()
            await asyncio.sleep(1)
            t = datetime.now() - prev_dt

            print(
                f"Total points: {total_points}, calculated val: {calculated_val}, "
                f"per second: {(total_points - prev_total)/t.total_seconds()} (loop: {t})"
            )
            prev_total = total_points

    asyncio.create_task(print_stats())

    async for i, val in aenumerate(api):
        total_points, calculated_val = i * 10000, val
        if i % 10000 == 0:
            print(i)
        # await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
