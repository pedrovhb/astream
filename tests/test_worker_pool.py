# from __future__ import annotations
#
# import asyncio
#
# import pytest
#
# from astream import arange
# from astream.worker_pool import WorkerPool
#
#
# @pytest.mark.asyncio
# async def test_worker_pool() -> None:
#     async def do_work(it: int) -> int:
#         return it * 2
#
#     wp = WorkerPool(do_work, 3)
#
#     expected = iter(range(10))
#
#     s = arange(10)
#     print(s)
#     print(type(s))
#     async for item in arange(10) / wp:
#         assert isinstance(item, int)
#         assert item == next(expected) * 2
