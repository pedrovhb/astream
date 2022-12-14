from __future__ import annotations

from datetime import timedelta
from itertools import chain, tee
from typing import AsyncIterable, Counter, Iterable, Iterator

import pytest
from hypothesis import given
from hypothesis import strategies as st

from astream import Stream
from astream.stream_utils import (
    aconcatenate,
    aenumerate,
    aflatmap,
    agetattr,
    agetitem,
    amap,
    amerge,
    arange,
    arange_delayed,
    arepeat,
    ascan,
    atee,
    azip,
    azip_longest,
)
from astream.transformer_utils import areduce


@pytest.mark.asyncio
async def test_amerge() -> None:
    ranges = arange(10), arange(100, 120), arange(200, 220)
    expected = Counter[int](chain(range(10), range(100, 120), range(200, 220)))

    actual = Counter[int]()
    async for i in amerge(*ranges):
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


@pytest.mark.asyncio
async def test_aconcatenate() -> None:
    expected = list(range(10))
    expected.extend(range(100, 120))
    expected.extend(range(200, 220))
    actual = []
    async for i in aconcatenate(
        arange(10),
        arange(100, 120),
        arange(200, 220),
    ):
        actual.append(i)

    assert actual == expected


# Tests for aenumerate, agetattr, atee, amap, aflatmap


@pytest.mark.asyncio
async def test_aenumerate() -> None:
    expected = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]
    async for i, j in aenumerate(arange(5)):
        assert i == expected[i][0]
        assert j == expected[i][1]


@pytest.mark.asyncio
async def test_agetattr() -> None:
    class Foo:
        def __init__(self, n: int) -> None:
            self.n = n

    expected = [0, 1, 2, 3, 4]
    async for i in agetattr(Stream([Foo(n) for n in expected]), "n"):
        assert i == expected.pop(0)


@pytest.mark.asyncio
async def test_atee() -> None:
    a, b = atee(arange(5), 2)

    g = iter(range(5))
    async for i in a:
        assert i == next(g)

    g = iter(range(5))
    async for i in b:
        assert i == next(g)

    with pytest.raises(StopIteration):
        next(g)


@pytest.mark.asyncio
async def test_amap() -> None:
    g = iter(range(5))
    async for i in amap(lambda x: x + 1, arange(5)):
        assert i == next(g) + 1

    with pytest.raises(StopIteration):
        next(g)


@pytest.mark.asyncio
async def test_aflatmap() -> None:
    async def range_stringer(ar: AsyncIterable[int]) -> AsyncIterable[str]:
        async for i in ar:
            yield str(i) + "!"

    def exp() -> Iterator[int]:
        for i in range(5):
            yield from range(i)

    async def iter_of_iters() -> AsyncIterable[AsyncIterable[int]]:
        for i in range(5):
            yield arange(i)

    gen = exp()
    async for j in aflatmap(range_stringer, iter_of_iters()):
        assert j == str(next(gen)) + "!"

    with pytest.raises(StopIteration):
        next(gen)


@pytest.mark.asyncio
async def test_flatten() -> None:

    expected = chain.from_iterable(range(i) for i in range(5))
    async for j in +Stream(amap(arange, arange(5))):
        assert j == next(expected)

    with pytest.raises(StopIteration):
        next(expected)


@pytest.mark.asyncio
async def test_arange_delayed() -> None:
    g = iter(range(5))
    async for i in arange_delayed(5, delay=0.1):
        assert i == next(g)

    with pytest.raises(StopIteration):
        next(g)


@pytest.mark.asyncio
async def test_azip_longest() -> None:
    def expected() -> Iterator[tuple[int, int, int]]:
        yield 10, 10, 15
        yield 11, 11, 16
        yield 12, 12, 17
        yield 13, 13, 18
        yield 14, 14, 19
        yield -1, -1, 20
        yield -1, -1, 21
        yield -1, -1, 22
        yield -1, -1, 23
        yield -1, -1, 24

    (a, b), c = atee(arange(10, 15), 2), arange(15, 25)
    exp = expected()
    async for tup in azip_longest(a, b, c, fillvalue=-1):
        assert tup == next(exp)

    with pytest.raises(StopIteration):
        next(exp)


@pytest.mark.asyncio
async def test_azip() -> None:
    def expected() -> Iterator[tuple[int, int, int]]:
        yield 10, 10, 15
        yield 11, 11, 16
        yield 12, 12, 17
        yield 13, 13, 18
        yield 14, 14, 19

    (a, b), c = atee(arange(10, 15), 2), arange(15, 25)
    exp = expected()
    async for tup in azip(a, b, c):
        assert tup == next(exp)

    with pytest.raises(StopIteration):
        next(exp)


@pytest.mark.asyncio
async def test_arepeat() -> None:
    def expected() -> Iterator[int]:
        for _ in range(4):
            yield from range(10, 15)

    exp = expected()
    async for tup in arepeat(arange(10, 15), 4):
        assert tup == next(exp)

    with pytest.raises(StopIteration):
        next(exp)
