from __future__ import annotations

import random
from collections.abc import AsyncIterable
from dataclasses import dataclass
from typing import TypeVar

import asyncio as asyncio
from rich import inspect
from rich.console import Console

from astream import arange, run_sync, arange_delayed
from astream.experimental.partializer import F
from astream.stream import Stream, StreamMappable
from astream.stream_grouper import Default, apredicate_multi_map, apredicate_map
from astream.subproc import Proc
from astream.transformer_utils import dotget


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

    st = Stream(db.iter_employees())

    def pegar_nome_empregado(it: Employee) -> str:
        return it.name

    def departamento_eh_ti(it: Employee) -> bool:
        return it.department == "IT"

    async for empregado in st % departamento_eh_ti / pegar_nome_empregado / (
        lambda nome: nome[::-1]
    ):
        print(empregado)


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


async def _aflatten() -> None:
    def tolist(it: int) -> list[int]:
        return list(range(it))

    def is_multiple_of(it: int, of: int) -> bool:
        return it % of == 0

    s = arange(100) / apredicate_multi_map(
        {
            F(is_multiple_of)(of=2): lambda it: it * 2,
            F(is_multiple_of)(of=3): lambda it: it * 3,
            Default: lambda it: it * 5,
        }
    )
    s = (
        arange(100)
        / (lambda it: (it,))
        / apredicate_multi_map(
            {
                (lambda n: n[0] % 3 == 0): lambda it: (*it, "fizz"),
                (lambda n: n[0] % 5 == 0): lambda it: (*it, "buzz"),
                Default: lambda it: it,
            }
        )
        / (lambda it: f"{it[0]} {''.join(it[1:])}")
    )

    return [it async for it in s]


@run_sync
async def aflatten() -> list[int]:
    return [it async for it in _aflatten()]


A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")


_T = TypeVar("_T")
_R = TypeVar("_R")


class Enumerator(StreamMappable[int, tuple[int, int]]):
    def __init__(self, start: int = 0) -> None:
        self.start = start

    def __stream_map__(self, s: Stream[int]) -> Stream[tuple[int, int]]:
        async def _stream_map() -> AsyncIterable[tuple[int, int]]:
            i = self.start
            async for item in s:
                yield i, item
                i += 1

        return Stream(_stream_map())


async def aflatten_example() -> None:
    async def to_range(m: int) -> AsyncIterable[int]:
        for i in range(m):
            yield i
            # await asyncio.sleep(0.1)

    def mul_two(x: int) -> int:
        return x * 2

    async def spell_out(x: int) -> tuple[int, str, str]:
        return x, "fizz" if x % 3 == 0 else "", "buzz" if x % 5 == 0 else ""

    def take_last_two(x: tuple[int, str, str]) -> list[int]:
        return [xx if isinstance(xx, int) else len(xx) for xx in x]

    def is_even(x: int) -> bool:
        return x % 2 == 0

    # sm = +st.aclone()
    import heartrate

    heartrate.trace(browser=True)
    while True:
        st = Stream(arange_delayed(10, delay=0.1)) / mul_two // arange / spell_out
        async for item in st / (lambda abc: abc[1] + abc[2]) / str.strip % bool / apredicate_map(
            {
                lambda it: it.startswith("f"): str.upper,
                Default: str.title,
            }
        ):
            print(item)

    # For android
    # package:/data/app/~~_6Hm0BCuyr_uMrBc65DBKw==/com.simplemobiletools.gallery-jF-48bOHm34ojGB0APcjdA==/base.apk=com.simplemobiletools.gallery


async def main() -> None:
    proc = await Proc.run_shell("sudo pm list packages -f")
    async for line in proc.stdout_decoded:
        _, path, package = line.rsplit("=", 1)
        print(path, package)


if __name__ == "__main__":
    import asyncio
    from astream import Stream
    from astream.subproc import Proc

    asyncio.run(main())


if __name__ == "__main__":
    import asyncio

    asyncio.run(aflatten_example())
    # asyncio.run())
