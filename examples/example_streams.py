from __future__ import annotations

import random
from collections.abc import AsyncIterable
from dataclasses import dataclass
from functools import partial
from types import FunctionType
from typing import Callable, Generic, TypeVar, ParamSpec, Iterable

from astream import stream, arange
from astream.experimental.partializer import F
from astream.experimental.surrogate import It
from astream.stream_utils import dotget


@dataclass
class Employee:
    name: str
    department: str
    likes: list[str]
    favorite_number: int


class EmployeeDB:

    employees = [
        Employee("Jane", "IT", ["pizza", "dogs"], 4),
        Employee("Jack", "IT", ["pizza", "cats"], 5),
        Employee("Bob", "HR", ["cats"], 8),
        Employee("Alice", "HR", ["cats", "dogs"], 6),
        Employee("Bob", "HR", ["cats", "pizza"], 0),
        Employee("John", "IT", ["programming"], 1),
        Employee("Alice", "HR", ["cats", "dogs", "pizza"], 4),
    ]

    async def iter_employees(self) -> AsyncIterable[Employee]:
        for employee in self.employees:
            yield employee
            await asyncio.sleep(random.uniform(0, 0.2))  # old system


async def main() -> None:
    db = EmployeeDB()

    st = stream(db.iter_employees())

    ss = st / (lambda e: print(e.name) or e) / (lambda e: e.likes) / print

    async for _ in ss:
        pass

    # What employees in IT like pizza?
    st = stream(db.iter_employees())
    async for employee in st % (lambda e: e.department == "IT") % (lambda e: "pizza" in e.likes):
        print(employee)

    # What employees in IT like pizza?
    st = stream(db.iter_employees())

    like_pizza = st % (lambda e: e.department == "IT") % (lambda e: "pizza" in e.likes)
    async for employee in like_pizza:
        print(employee, "likes pizza and is in IT")


async def adot() -> None:
    async def data_generator() -> AsyncIterable[dict]:
        """Generate some random data for testing.

        Example output:
            data = {
                "a": {
                    "b": {"foo": 1, "bar": 2},
                    "c": 2,
                    "d": [1, {"abc": [45, "ghi"]}, 3],
                    "e": {"foo": 1, "baz": 2},
                },
                "d": 3,
            }
        """
        while True:
            data = {
                "a": {
                    "b": {"foo": random.randint(1, 10), "bar": random.randint(1, 10)},
                    "c": random.randint(1, 10),
                    "d": [
                        random.randint(1, 10),
                        {"abc": [random.randint(1, 10), "ghi"]},
                        random.randint(1, 10),
                    ],
                    "e": {"foo": random.randint(1, 10), "baz": random.randint(1, 10)},
                },
                "d": random.randint(1, 10),
            }
            yield data
            await asyncio.sleep(0.1)

    async for data in stream(data_generator()) // dotget("a.b.foo"):
        print(data)


async def aflatten() -> None:
    def tolist(it: int) -> list[int]:
        return list(range(it))

    s = arange(10) / F(tolist)
    reveal_type(s)
    ss = +s
    reveal_type(ss)


if __name__ == "__main__":
    import asyncio

    asyncio.run(aflatten())
    # asyncio.run())
