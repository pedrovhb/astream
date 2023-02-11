from itertools import chain
from typing import AsyncIterator

import pytest

from astream.emitters import merge_async_iterables


async def _empty_aiter() -> AsyncIterator[int]:
    if False:
        yield 0


async def _aiter_raises() -> AsyncIterator[int]:
    if False:
        yield 0
    raise ValueError("test")


async def arange(i) -> AsyncIterator[int]:
    for j in range(i):
        yield j


@pytest.mark.asyncio
async def test_merge_async_iterables():

    async_iters = [arange(10), arange(5), arange(15)]
    async_iter = merge_async_iterables(*async_iters)
    merged = [i async for i in async_iter]
    expected = list(chain(range(15), range(10), range(5)))

    merged.sort()
    expected.sort()
    assert merged == expected


@pytest.mark.asyncio
async def test_merge_async_iterables_no_async_iterable():
    async_iter = merge_async_iterables()
    merged = [i async for i in async_iter]
    assert merged == []


@pytest.mark.asyncio
async def test_merge_async_iterables_empty_async_iterable():

    async_iter = merge_async_iterables(_empty_aiter())
    merged = [i async for i in async_iter]
    assert merged == []


@pytest.mark.asyncio
async def test_merge_async_iterables_never_produces_async_iterable():
    async_iter = merge_async_iterables(_empty_aiter(), _empty_aiter())
    merged = [i async for i in async_iter]
    assert merged == []


@pytest.mark.asyncio
async def test_merge_async_iterables_raises_exception_async_iterable():
    async_iter = merge_async_iterables(_aiter_raises(), _empty_aiter(), arange(10))
    with pytest.raises(Exception):
        merged = [i async for i in async_iter]
