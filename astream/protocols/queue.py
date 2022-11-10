from __future__ import annotations

from typing import Protocol

from astream import T_contra, T_co


class QueueProtocol(Protocol[T_contra, T_co]):

    async def put(self, item: T_contra) -> None: ...

    async def get(self) -> T_co: ...

    def put_nowait(self, item: T_contra) -> None: ...

    def get_nowait(self) -> T_co: ...
