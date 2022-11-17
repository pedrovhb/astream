from __future__ import annotations

from operator import getitem

from loguru import logger

from astream import Stream
from astream.experimental.partializer import F
from astream.experimental.simple_surrogate import it
from astream.stream_grouper import apredicate_map


async def group_f() -> None:
    logger.info("group_f")

    data = [
        {"vehicle": "car", "max_speed": 200, "n_wheels": 4, "n_doors": 4},
        {"vehicle": "bike", "max_speed": 60, "n_wheels": 2, "n_doors": 0},
        {"vehicle": "truck", "max_speed": 120, "n_wheels": 8, "n_doors": 2},
        {"vehicle": "bus", "max_speed": 100, "n_wheels": 6, "n_doors": 2},
        {"vehicle": "motorcycle", "max_speed": 180, "n_wheels": 2, "n_doors": 0},
        {"vehicle": "boat", "max_speed": 50, "n_wheels": 0, "n_doors": 0},
        {"vehicle": "plane", "max_speed": 800, "n_wheels": 3, "n_doors": 1},
        {"vehicle": "train", "max_speed": 200, "n_wheels": 0, "n_doors": 0},
    ]

    def double_spd(x):
        x["max_speed"] *= 2
        return x

    s = Stream(data) / apredicate_map(
        {
            F(it["n_wheels"] == 0): F(it["max_speed"]),
            F(it["n_wheels"] == 2): lambda x: {**x, "type": "bicycle"},
            F(it["n_wheels"] == 4): double_spd,
            it["n_wheels"] > 4: F(it["vehicle"]),
        }
    )

    async for v in s:
        logger.info(v)


if __name__ == "__main__":
    import asyncio

    asyncio.run(group_f())
