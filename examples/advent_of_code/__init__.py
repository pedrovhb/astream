from __future__ import annotations


from . import day_1
from . import day_2


challenges = [[day_1.part_1, day_1.part_2], [day_2.part_1]]


async def show_challenges() -> None:

    for day, parts in enumerate(challenges, 1):
        print(f"======== Day {day} ========\n")

        for part, func in enumerate(parts, 1):
            print(f" -  = Part {part} = - -")
            result = await func()
            print(f" -  = Result: {result} = - -\n")


if __name__ == "__main__":
    import asyncio

    asyncio.run(show_challenges(), debug=True)
