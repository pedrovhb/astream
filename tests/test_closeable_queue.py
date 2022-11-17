from __future__ import annotations

import asyncio

import pytest

from astream import closeable_queue
from astream.closeable_queue import CloseableQueue


# Test the CloseableQueue class in isolation


@pytest.mark.asyncio
async def test_closeable_queue() -> None:
    queue = closeable_queue.CloseableQueue[int]()
    await queue.put(1)
    queue.close()
    assert queue.is_closed
    assert not queue.is_exhausted
    assert queue.qsize() == 1

    assert await queue.get() == 1
    queue.task_done()

    assert queue.qsize() == 0
    assert queue.is_exhausted

    with pytest.raises(closeable_queue.QueueExhausted):  # type: ignore
        await queue.get()

    with pytest.raises(closeable_queue.QueueExhausted):
        queue.get_nowait()

    with pytest.raises(closeable_queue.QueueClosed):
        await queue.put(2)

    with pytest.raises(closeable_queue.QueueClosed):
        queue.put_nowait(2)

    assert queue.is_closed
    assert queue.is_exhausted


@pytest.mark.asyncio
async def test_closeable_queue_wait_closed() -> None:

    do_close = asyncio.Future[None]()
    queue = closeable_queue.CloseableQueue[int]()

    async def wait_closed() -> None:
        assert not queue.is_closed
        do_close.set_result(None)
        await queue.wait_closed()
        assert queue.is_closed
        assert not queue.is_exhausted  # type: ignore

    task = asyncio.create_task(wait_closed())
    await queue.put(1)
    await do_close
    queue.close()
    await task


@pytest.mark.asyncio
async def test_closeable_queue_wait_exhausted() -> None:

    do_grab = asyncio.Future[None]()
    queue = closeable_queue.CloseableQueue[int]()
    await queue.put(1)
    await queue.put(2)

    async def grabber() -> None:
        await do_grab
        with pytest.raises(closeable_queue.QueueExhausted):
            await queue.get()

    grabbers = [asyncio.create_task(grabber()) for _ in range(5)]
    await queue.get()
    queue.task_done()
    queue.close()
    await queue.get()
    queue.task_done()

    do_grab.set_result(None)
    await asyncio.gather(*grabbers)

    with pytest.raises(closeable_queue.QueueExhausted):
        queue.get_nowait()

    await queue.wait_exhausted()
    assert queue.is_closed
    assert queue.is_exhausted


@pytest.mark.asyncio
async def test_closeable_queue_exhausted_when_closed() -> None:
    """Queue is immediately marked as exhausted when closed and empty."""
    queue = closeable_queue.CloseableQueue[int]()
    await queue.put(1)
    await queue.get()
    queue.task_done()

    queue.close()
    assert queue.is_closed
    assert queue.is_exhausted


@pytest.mark.asyncio
async def test_closeable_queue_exhausted_when_closed_and_getnowait() -> None:
    """Queue is immediately marked as exhausted when closed and empty."""
    queue = closeable_queue.CloseableQueue[int]()
    await queue.put(1)
    queue.close()
    assert queue.get_nowait() == 1
    queue.task_done()

    assert queue.is_closed
    assert queue.is_exhausted


@pytest.mark.asyncio
async def test_closeable_queue_exhausted_when_getnowait_empty() -> None:
    queue = closeable_queue.CloseableQueue[int]()
    await queue.put(1)
    queue.close()
    assert await queue.get() == 1
    queue.task_done()

    with pytest.raises(closeable_queue.QueueExhausted):
        queue.get_nowait()

    assert queue.is_closed
    assert queue.is_exhausted


@pytest.mark.asyncio
async def test_closeable_queue_has_waiters_and_is_closed() -> None:
    queue = closeable_queue.CloseableQueue[int]()

    async def grabber(grab_fut: asyncio.Future[None]) -> None:
        with pytest.raises(closeable_queue.QueueExhausted):
            task = asyncio.create_task(queue.get())
            grab_fut.set_result(None)
            await task

    grab_ok = [asyncio.Future[None]() for _ in range(5)]
    grabbers = [asyncio.create_task(grabber(gok)) for gok in grab_ok]

    await asyncio.wait(grab_ok, return_when=asyncio.ALL_COMPLETED)
    queue.close()
    await asyncio.wait(grabbers)


@pytest.mark.asyncio
async def test_closeable_queue_has_putters_and_is_exhausted() -> None:
    queue = closeable_queue.CloseableQueue[int](maxsize=2)

    async def putter(put_fut: asyncio.Future[None]) -> None:
        with pytest.raises(closeable_queue.QueueClosed):
            put_fut.set_result(None)
            await queue.put(1)

    put_ok = [asyncio.Future[None]() for _ in range(5)]
    putters = [asyncio.create_task(putter(pok)) for pok in put_ok]

    await asyncio.wait(put_ok, return_when=asyncio.ALL_COMPLETED)
    assert queue.qsize() == 2

    queue.close()
    await asyncio.wait(putters)


@pytest.mark.asyncio
async def test_closeable_queue_async_iter() -> None:
    queue = closeable_queue.CloseableQueue[int]()
    await queue.put(1)
    await queue.put(2)
    await queue.put(3)
    queue.close()

    results = []
    async for item in queue:
        results.append(item)

    assert results == [1, 2, 3]
    assert queue.is_closed
    assert queue.is_exhausted


@pytest.mark.asyncio
async def test_closeable_queue_async_iter_with_exception() -> None:
    queue = closeable_queue.CloseableQueue[int](maxsize=5)

    async def fill_queue() -> None:
        for i in range(10):
            await queue.put(i)
        queue.close()

    asyncio.create_task(fill_queue())

    expected = iter(range(10))
    async for item in queue:
        # print(item)  # 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        assert item == next(expected)


@pytest.mark.asyncio
async def test_closeable_queue_async_iter_with_exception() -> None:
    queue = closeable_queue.CloseableQueue[int](maxsize=5)

    async def fill_queue() -> None:
        for i in range(10):
            await queue.put(i)
        queue.close()

    asyncio.create_task(fill_queue())

    expected = iter(range(10))
    async for item in queue:
        # print(item)  # 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        assert item == next(expected)
