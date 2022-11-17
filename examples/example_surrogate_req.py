from __future__ import annotations

from operator import getitem
from typing import Any, AsyncIterable

import httpx
import rich
from loguru import logger

from astream import arange, atee
from astream.experimental.partializer import F
from astream.experimental.simple_surrogate import it
from astream.stream_grouper import Default, agroup_map, apredicate_multi_map

client = httpx.AsyncClient()


async def get_page_items(page_number: int) -> AsyncIterable[dict[str, Any]]:

    req = await client.get(
        "https://api.github.com/search/repositories?q=language:python&sort=stars&page={}".format(
            page_number
        )
    )
    try:
        for item in req.json()["items"]:
            yield item
    except Exception as e:
        logger.exception(e)
        raise


async def main() -> None:

    s = arange(3) // get_page_items
    original, modificada = atee(s, 2)

    async for v in original:
        rich.print(v)

    async for v in modificada / F(it["full_name"].split("/")[1]):
        rich.print(v)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
