from __future__ import annotations

import asyncio

import pytest

from astream.stream_utils import arange
from astream.worker_pool import WorkerPool


@pytest.mark.asyncio
async def test_worker_pool() -> None:
    async def do_work(it: int) -> int:
        return it * 2

    wp = WorkerPool(do_work, 3)

    tasks = []
    async for item in arange(10):
        result = wp(item)
        tasks.append(result)

    results = await asyncio.gather(*tasks)
    assert results == [i * 2 for i in range(10)]
    await wp.close()
