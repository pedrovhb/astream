from __future__ import annotations

from typing import AsyncIterable, Iterable, Callable, TypeAlias, Any, Coroutine, Iterator, TypeVar

import pytest

from astream import Stream

_T = TypeVar("_T")
_CoroT: TypeAlias = Coroutine[Any, Any, _T]


def _sync_function(item: int) -> int:
    return item * 2


async def _async_function(item: int) -> int:
    return item * 2


def _sync_iterable() -> Iterable[int]:
    return range(10)


async def _async_iterable() -> AsyncIterable[int]:
    for i in range(10):
        yield i


def expected_range() -> Iterator[int]:
    return iter(range(0, 20, 2))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "src, fn, expected",
    [
        (_sync_iterable(), _sync_function, expected_range()),
        (_async_iterable(), _sync_function, expected_range()),
        (_sync_iterable(), _async_function, expected_range()),
        (_async_iterable(), _async_function, expected_range()),
    ],
)
async def test_map(
    src: Iterable[int] | AsyncIterable[int],
    fn: Callable[[int], int] | Callable[[int], _CoroT[int]],
    expected: Iterator[int],
) -> None:
    async for i in Stream(src) / fn:
        assert i == next(expected)

    with pytest.raises(StopIteration):
        next(expected)


def _range_maker(num: int) -> Iterable[int]:
    return range(num)


async def _async_range_maker(num: int) -> AsyncIterable[int]:
    for i in range(num):
        yield i


async def _coro_range_maker(num: int) -> Iterable[int]:
    return _range_maker(num)


def _generator_range_maker(num: int) -> Iterable[int]:
    # for i in range(num):
    #     yield from range(i)
    yield from range(num)


def expected_nested_range() -> Iterator[int]:
    for i in range(10):
        yield from range(i)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "src, fn, expected",
    [
        (_sync_iterable(), _range_maker, expected_nested_range()),
        (_async_iterable(), _range_maker, expected_nested_range()),
        (_sync_iterable(), _async_range_maker, expected_nested_range()),
        (_async_iterable(), _async_range_maker, expected_nested_range()),
        (_sync_iterable(), _coro_range_maker, expected_nested_range()),
        (_async_iterable(), _coro_range_maker, expected_nested_range()),
        (_sync_iterable(), _generator_range_maker, expected_nested_range()),
        (_async_iterable(), _generator_range_maker, expected_nested_range()),
    ],
)
async def test_flat_map(
    src: Iterable[int] | AsyncIterable[int],
    fn: Callable[[int], Iterable[int]] | Callable[[int], AsyncIterable[int]],
    expected: Iterator[int],
) -> None:
    async for i in Stream(src) // fn:
        assert i == next(expected)

    with pytest.raises(StopIteration):
        next(expected)


@pytest.mark.asyncio
async def test_flat_map_invalid() -> None:
    with pytest.raises(TypeError):
        async for _ in Stream(_sync_iterable()) // _sync_function:  # type: ignore
            pass


def _sync_filter(item: int) -> bool:
    return item % 2 == 0


async def _async_filter(item: int) -> bool:
    return item % 2 == 0


def expected_filtered_range() -> Iterator[int]:
    return iter(range(0, 10, 2))


def expected_filtered_false_range() -> Iterator[int]:
    return iter(range(1, 11, 2))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "src, fn, expected",
    [
        (_sync_iterable(), _sync_filter, expected_filtered_range()),
        (_async_iterable(), _sync_filter, expected_filtered_range()),
        (_sync_iterable(), _async_filter, expected_filtered_range()),
        (_async_iterable(), _async_filter, expected_filtered_range()),
    ],
)
async def test_filter(
    src: Iterable[int] | AsyncIterable[int],
    fn: Callable[[int], bool] | Callable[[int], _CoroT[bool]],
    expected: Iterator[int],
) -> None:
    async for i in Stream(src) % fn:
        assert i == next(expected)

    with pytest.raises(StopIteration):
        next(expected)


#
# @pytest.mark.asyncio
# @pytest.mark.parametrize(
#     "src, fn, expected",
#     [
#         (_sync_iterable(), _sync_filter, expected_filtered_false_range()),
#         (_async_iterable(), _sync_filter, expected_filtered_false_range()),
#         (_sync_iterable(), _async_filter, expected_filtered_false_range()),
#         (_async_iterable(), _async_filter, expected_filtered_false_range()),
#     ],
# )
# async def test_filter_not(
#     src: Iterable[int] | AsyncIterable[int],
#     fn: Callable[[int], bool] | Callable[[int], _CoroT[bool]],
#     expected: Iterator[int],
# ) -> None:
#     async for i in src % FilterFalse(fn):
#         assert i == next(expected)
#
#     with pytest.raises(StopIteration):
#         next(expected)
