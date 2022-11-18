from __future__ import annotations

import asyncio
import random
from collections import deque
from datetime import timedelta
from statistics import mean
from typing import Generic, Iterator, Sequence, TypeVar

from loguru import logger

from astream import Stream, arange_delayed

_T = TypeVar("_T")


class TimeDeque(Generic[_T]):
    def __init__(self, n_bins: int, bin_size: timedelta | int) -> None:
        self.n_bins = n_bins
        self.bin_size = bin_size if isinstance(bin_size, timedelta) else timedelta(seconds=bin_size)
        self.bins: deque[deque[_T]] = deque(maxlen=n_bins)
        for _ in range(n_bins):
            self.bins.append(deque[_T]())

        loop = asyncio.get_event_loop()
        self.start_time = loop.time()
        self._iters = 0
        loop.call_at(
            self.start_time + (self._iters * self.bin_size.total_seconds() + 1),
            self._rotate
        )


    def __len__(self) -> int:
        return self.n_bins

    def __getitem__(self, index: int) -> deque[_T]:
        return self.bins[index]

    def __iter__(self) -> Iterator[deque[_T]]:
        return iter(self.bins)

    def _rotate(self) -> None:
        self._iters += 1
        loop = asyncio.get_event_loop()
        loop.call_at(
            self.start_time + (self._iters * self.bin_size.total_seconds() + 1),
            self._rotate
        )
        drift = loop.time() - self.start_time - self._iters * self.bin_size.total_seconds()
        logger.info(f"Rotated at {loop.time()} (drift: {drift})")
        self.bins.appendleft(deque[_T]())

    def append(self, item: _T) -> None:
        self.bins[0].appendleft(item)

    def __stream_map__(self, stream: Stream[_T]) -> Stream[_T]:

        def _mapper(item: _T) -> _T:
            self.append(item)
            return item
        s = stream / _mapper
        setattr(s, "_timedeque", self)
        return s



async def main() -> None:
    td = TimeDeque[int](5, 1)
    st = arange_delayed(100, delay=0.1) / td
    async for i in st:
        await asyncio.sleep(0.1)
        print(i, st._timedeque.bins)

if __name__ == '__main__':
    asyncio.run(main())
