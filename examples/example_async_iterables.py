import asyncio
import random
from collections.abc import AsyncIterable

from asyncutils.amerge import amerge


async def arange(start: int, stop: int) -> AsyncIterable[int]:
    for i in range(start, stop):
        await asyncio.sleep(random.uniform(0, 0.2))
        yield i


async def arange_alphabet(start: int, stop: int) -> AsyncIterable[str]:
    for i in range(start, stop):
        await asyncio.sleep(random.uniform(0, 0.2))
        yield chr(ord("a") + i % 26)


async def example_amerge() -> None:
    async for item in (merged := amerge[int | str](arange(0, 5), arange(5, 10))):
        print(item)

        # Type information is kept:
        # reveal_type(item)  # Revealed type is "builtins.int"

        if item == 7:
            # Now let's...
            # merged.add_async_iter(arange_alphabet(0, 5))
            # Oops, that doesn't match our iterator's type! Mypy warns us about it:
            # Mypy: Argument 1 to "add_async_iter" of "MergedAsyncIterable" has incompatible type
            # "AsyncIterable[str]"; expected "AsyncIterable[int]" [arg-type]

            # We could've made that ok by specifying in the merged iterator's creation that we
            # intended for the type to be a union:
            #  (merged := amerge[int | str](arange(0, 5), arange(5, 10))
            # But we'll go ahead and just add an int iterator:
            merged.add_async_iter(arange(1337, 1340))


if __name__ == "__main__":
    asyncio.run(example_amerge())
