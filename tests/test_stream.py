from __future__ import annotations

import pytest

from asyncutils.stream import Stream
from asyncutils.utils import arange


# Test the CloseableQueue class in isolation


@pytest.mark.asyncio
async def test_stream() -> None:
    expected = iter(range(10))
    async for i in arange(10):
        assert i == next(expected)

    assert next(expected, None) is None


@pytest.mark.asyncio
async def test_stream_map() -> None:
    expected = iter(map(lambda x: x * 2, range(10)))
    async for i in (arange(10) / (lambda x: x * 2)):
        assert i == next(expected)

    assert next(expected, None) is None


@pytest.mark.asyncio
async def test_stream_filter() -> None:
    expected = iter(filter(lambda x: x % 2 == 0, range(10)))
    async for i in (arange(10) % (lambda x: x % 2 == 0)):
        assert i == next(expected)

    assert next(expected, None) is None
