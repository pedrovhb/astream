from __future__ import annotations

from datetime import timedelta
from itertools import tee
from typing import Counter, Iterable

import pytest

from astream.stream import Stream
from astream.stream_utils import agetitem, amerge, arange, areduce, ascan
from hypothesis import given, strategies as st


@pytest.mark.asyncio
@given(its=st.lists(st.iterables(st.integers(), max_size=1000)))
async def test_amerge(its: Iterable[timedelta]) -> None:
    tees: list[tuple[Iterable[int], Iterable[int]]] = [tee(it, 2) for it in its]  # type: ignore
    to_async = [Stream(it) for it, _ in tees]
    to_sync = [it for _, it in tees]
    expected = Counter[int]()
    for it in to_sync:
        expected.update(it)

    actual = Counter[int]()
    async for i in amerge(*to_async):
        actual[i] += 1

    assert actual == expected


@pytest.mark.asyncio
@given(its=st.iterables(st.lists(st.integers(), min_size=1, max_size=1000)))
async def test_apluck(its: list[list[int]]) -> None:

    async for item in agetitem(Stream(its), 0):
        assert isinstance(item, int)


@pytest.mark.asyncio
async def test_ascan() -> None:

    expected = [0, 1, 3, 6, 10, 15, 21, 28, 36, 45]
    async for item in ascan(lambda x, y: x + y, arange(10)):
        assert isinstance(item, int)
        assert item == expected.pop(0)
    assert not expected


@pytest.mark.asyncio
async def test_arange() -> None:

    expected = list(range(10))
    async for item in arange(10):
        assert isinstance(item, int)
        assert item == expected.pop(0)
    assert not expected


@pytest.mark.asyncio
async def test_arange_with_step() -> None:

    expected = list(range(0, 10, 2))
    async for item in arange(0, 10, 2):
        assert isinstance(item, int)
        assert item == expected.pop(0)
    assert not expected


@pytest.mark.asyncio
async def test_arange_with_negative_step() -> None:

    expected = list(range(10, 0, -2))
    async for item in arange(10, 0, -2):
        assert isinstance(item, int)
        assert item == expected.pop(0)
    assert not expected


@pytest.mark.asyncio
async def test_arange_with_negative_start() -> None:

    expected = list(range(-10, 0))
    async for item in arange(-10, 0):
        assert isinstance(item, int)
        assert item == expected.pop(0)
    assert not expected


@pytest.mark.asyncio
async def test_arange_with_negative_start_and_step() -> None:

    expected = list(range(-10, 0, 2))
    async for item in arange(-10, 0, 2):
        assert isinstance(item, int)
        assert item == expected.pop(0)
    assert not expected


@pytest.mark.asyncio
async def test_areduce() -> None:

    expected = 45
    result = await areduce(lambda x, y: x + y, arange(10))
    assert expected == result
