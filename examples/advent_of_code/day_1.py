import asyncio
from typing import *
from pathlib import Path

from astream.sources import from_file

from astream.stream_utils import partition_by_element, top_k

_T = TypeVar("_T")


async def part_1() -> int:
    file_path = Path(__file__).parent / "inputs/day_1_input"
    elf_carries = (
        from_file(file_path)
        / partition_by_element("")
        / (lambda l: [int(n) for n in l])
        / (lambda l: sum(l))
    )
    # reveal_type(elf_carries)  # Revealed type is "astream.stream.Stream[builtins.int]"
    result = await elf_carries.reduce(max)
    return result  # 67658


async def part_2() -> int:
    file_path = Path(__file__).parent / "inputs/day_1_input"
    elf_carries = (
        from_file(file_path)
        / partition_by_element("")
        / (lambda l: (int(n) for n in l))
        / (lambda l: sum(l))
        / top_k(3)
    )
    calories = await elf_carries  # Consumes the stream and takes the last item
    print(f"Calories for the top 3 elves: {calories}")
    # reveal_type(calories)  # Revealed type is "builtins.tuple[builtins.int, ...]"

    result = sum(calories)
    print(f"Total calories carried by them: {result}")
    return result  # 200158


if __name__ == "__main__":
    asyncio.run(part_1(), debug=True)
