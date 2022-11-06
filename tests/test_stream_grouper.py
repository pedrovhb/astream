from __future__ import annotations

import asyncio

import pytest

from astream.stream import Stream
from astream.stream_grouper import StreamGrouper
from astream.stream_utils import arange


@pytest.mark.asyncio
async def test_stream_grouper() -> None:
    async def _grouping_function(item: int) -> int:
        return item % 3

    stream_grouper_2 = StreamGrouper(_grouping_function, arange(100, 200))

    async def show_values(k: int) -> list[int]:
        values = []
        async for it in stream_grouper_2[k]:
            values.append(it)
        return values

    values_shower = []
    async for key in stream_grouper_2.akeys():
        task = asyncio.create_task(show_values(key))
        values_shower.append(task)

    vals = await asyncio.gather(*values_shower)
    assert vals == [
        list(range(100, 200, 3)),
        list(range(101, 200, 3)),
        list(range(102, 200, 3)),
    ]


@pytest.mark.asyncio
async def test_stream_grouper_close() -> None:
    async def _grouping_function(item: int) -> int:
        return item % 3

    stream_grouper_2 = StreamGrouper(_grouping_function, arange(100, 200))

    expected = set(range(100, 200, 3))
    actual = set[int]()

    async for i in stream_grouper_2.get_wait(1):
        actual.add(i)

    for key in stream_grouper_2:
        assert key in (0, 1, 2)

    assert actual == expected

    assert len(stream_grouper_2) == 3
    assert 0 in stream_grouper_2

    assert isinstance(stream_grouper_2[0], Stream)
    with pytest.raises(KeyError):
        _ = stream_grouper_2[3]
