from __future__ import annotations

from typing import Callable, Coroutine

import pytest

from examples.advent_of_code import day_1, day_2


@pytest.mark.parametrize(
    ("challenge", "result"),
    (
        (day_1.part_1, 67658),
        (day_1.part_2, 200158),
        (day_2.part_1, 12645),
    ),
)
@pytest.mark.asyncio
async def test_day(challenge: Callable[[], Coroutine[object, object, int]], result: int) -> None:
    assert await challenge() == result
