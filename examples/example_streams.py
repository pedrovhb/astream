from __future__ import annotations

import random
from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import NamedTuple

from astream.stream import Stream


@dataclass
class Employee:
    name: str
    department: str
    likes: list[str]


class EmployeeDB:

    employees = [
        Employee("Jane", "IT", ["pizza", "dogs"]),
        Employee("Jack", "IT", ["pizza", "cats"]),
        Employee("Bob", "HR", ["cats"]),
        Employee("Alice", "HR", ["cats", "dogs"]),
        Employee("Bob", "HR", ["cats", "pizza"]),
        Employee("John", "IT", ["programming"]),
        Employee("Alice", "HR", ["cats", "dogs", "pizza"]),
    ]

    async def iter_employees(self) -> AsyncIterable[Employee]:
        for employee in self.employees:
            yield employee
            await asyncio.sleep(random.uniform(0, 0.2))  # old system


async def main() -> None:
    db = EmployeeDB()

    # What employees in IT like pizza?
    stream = Stream(db.iter_employees())
    async for employee in stream % (lambda e: e.department == "IT") % (
        lambda e: "pizza" in e.likes
    ):
        print(employee)

    # What employees in IT like pizza?
    stream = Stream(db.iter_employees())

    like_pizza = stream % (lambda e: e.department == "IT") % (lambda e: "pizza" in e.likes)
    async for employee in like_pizza @ sum:
        print(employee.name)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
