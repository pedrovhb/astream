from __future__ import annotations

import pytest

from astream import Stream
from astream.stream_utils import amerge, arange

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
    async for i in Stream(arange(10)) / (lambda x: x * 2):
        assert i == next(expected)

    assert next(expected, None) is None


@pytest.mark.asyncio
async def test_stream_filter() -> None:
    expected = iter(filter(lambda x: x % 2 == 0, range(10)))
    async for i in Stream(arange(10)) % (lambda x: x % 2 == 0):
        assert i == next(expected)

    assert next(expected, None) is None


@pytest.mark.asyncio
async def test_amerge() -> None:
    expected = set(range(10))
    expected.update(range(100, 120))
    expected.update(range(200, 220))
    actual = set[int]()
    async for i in amerge(
        arange(10),
        arange(100, 120),
        arange(200, 220),
    ):
        actual.add(i)

    assert actual == expected
