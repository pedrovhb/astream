import asyncio
from typing import Iterator

from astream import stream


@stream
def trange(__start: int, __stop: int, __step: int = 1) -> Iterator[int]:
    return iter(range(__start, __stop, __step))


async def main() -> None:
    tr = trange(0, 10)
    async for i in tr / str:
        reveal_type(tr)
        reveal_type(i)

        print(i)


if __name__ == "__main__":
    asyncio.run(main())
