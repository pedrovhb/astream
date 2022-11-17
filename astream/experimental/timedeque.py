from __future__ import annotations

from collections import deque
from datetime import timedelta
from typing import Generic, Iterator, Sequence, TypeVar

from loguru import logger

_T = TypeVar("_T")


class TimeDeque(Generic[_T], Sequence[_T]):
    def __init__(self, n_bins: int, bin_size: timedelta | int) -> None:
        self.n_bins = n_bins
        self.bin_size = bin_size if isinstance(bin_size, timedelta) else timedelta(seconds=bin_size)
        self.bins: deque[_T] = deque()

    def __len__(self) -> int:
        return self.n_bins

    def __getitem__(self, index: int) -> _T:
        return self.bins[index]

    def __iter__(self) -> Iterator[_T]:
        return iter(self.bins)
