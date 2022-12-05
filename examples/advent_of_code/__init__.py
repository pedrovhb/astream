from __future__ import annotations

import os
import sys

from day_1 import part_1 as day_1_part_1, part_2 as day_1_part_2


challenges = [
    [day_1_part_1, day_1_part_2],
]


async def show_challenges() -> None:

    w = terminal_size()
    for day, parts in enumerate(challenges, 1):
        print(f"======== Day {day} ========\n")

        for part, func in enumerate(parts, 1):
            print(f" -  = Part {part} = - -")
            result = await func()
            print(f" -  = Result: {result} = - -\n")


if __name__ == "__main__":
    import asyncio

    asyncio.run(show_challenges(), debug=True)
