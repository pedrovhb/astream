from __future__ import annotations

from typing import Any, Callable, TypeVar, Iterable

import pytest
import pytest_asyncio
from hypothesis import given

from asyncutils import reactive
import asyncio
import hypothesis.strategies as st

import pytest

# All test coroutines will be treated as marked.
pytestmark = pytest.mark.asyncio
T = TypeVar("T")

DEFAULT_VALUE = 42
DEFAULT_FACTORY_VALUE = 43


class Test:

    prop = reactive.ReactiveProperty[int](DEFAULT_VALUE)
    prop2 = reactive.ReactiveProperty[int](default_factory=lambda: DEFAULT_FACTORY_VALUE)


@given(st.integers())
async def test_reactive_property(num: int) -> None:

    test = Test()
    assert test.prop == DEFAULT_VALUE
    test.prop = 2
    assert test.prop == 2

    assert test.prop2 == DEFAULT_FACTORY_VALUE
    test.prop2 = 2
    assert test.prop2 == 2


@given(st.lists(st.integers()))
async def test_reactive_property_changes(num_iterables: Iterable[int]) -> None:

    test = Test()

    async def gather_changes() -> list[int]:
        changes = []
        try:
            async for change in test.prop.changes():
                changes.append(change)
        except asyncio.CancelledError:
            pass
        return changes

    task = asyncio.create_task(gather_changes())

    assert test.prop == DEFAULT_VALUE
    test.prop = 2
    assert test.prop == 2

    iterable_values = []
    for val in num_iterables:
        iterable_values.append(val)
        test.prop = val

    await asyncio.sleep(0.1)
    task.cancel()
    assert await task == [2] + iterable_values
