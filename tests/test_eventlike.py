from __future__ import annotations

import itertools
from typing import Literal

import pytest

from event_like import EventfulCounter


@pytest.fixture
def counter() -> EventfulCounter:
    return EventfulCounter(min_value=0, max_value=10, clamp_to_bounds=False)


@pytest.fixture
def counter_clamped() -> EventfulCounter:
    return EventfulCounter(min_value=0, max_value=10, clamp_to_bounds=True)


@pytest.fixture
def counter_no_min() -> EventfulCounter:
    return EventfulCounter(min_value=None, max_value=10, clamp_to_bounds=False)


@pytest.fixture
def counter_no_max() -> EventfulCounter:
    return EventfulCounter(min_value=0, max_value=None, clamp_to_bounds=False)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "max_val, min_val, clamp, value, mode",
    itertools.product(
        [10, None],
        [0, None],
        [True, False],
        [0, 5, 10, 15, -5],
        ["set", "inc", "dec", "iadd", "isub"],
    ),
)
async def test_set(
    max_val: float | None,
    min_val: float | None,
    clamp: bool,
    value: float,
    mode: Literal["set", "inc", "dec", "iadd", "isub"],
) -> None:
    counter = EventfulCounter(
        initial_value=0,
        max_value=max_val,
        min_value=min_val,
        clamp_to_bounds=clamp,
    )

    if mode == "set":
        expected = value
        counter.set(value)
    elif mode in ("inc", "iadd"):
        expected = 0
        for _ in range(int(value)):
            expected += 1
            if mode == "inc":
                counter.inc()
            else:
                counter += 1

    elif mode in ("dec", "isub"):
        expected = 0
        for _ in range(abs(int(value))):
            expected -= 1
            if mode == "dec":
                counter.dec()
            else:
                counter -= 1
    else:
        raise ValueError(f"Unknown mode: {mode}")

    if clamp and min_val is not None:
        expected = max(expected, min_val)
    if clamp and max_val is not None:
        expected = min(expected, max_val)

    assert float(counter) == expected
    if min_val is not None:
        assert counter.min_value == min_val
        assert counter.min_is_set() == (float(counter) <= float(min_val))
    else:
        assert counter.min_is_set() is False

    if max_val is not None:
        assert counter.max_value == max_val
        assert counter.max_is_set() == (float(counter) >= float(max_val))
    else:
        assert counter.max_is_set() is False
