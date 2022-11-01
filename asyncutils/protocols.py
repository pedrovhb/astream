from __future__ import annotations

from typing import TypeVar, Protocol

_InputT = TypeVar("_InputT", contravariant=True)
_OutputT = TypeVar("_OutputT", covariant=True)


class QueueLike(Protocol[_InputT, _OutputT]):
    async def put(self, item: _InputT) -> None:
        ...

    def put_nowait(self, item: _InputT) -> None:
        ...

    async def get(self) -> _OutputT:
        ...

    def get_nowait(self) -> _OutputT:
        ...

    async def join(self) -> None:
        ...

    def empty(self) -> bool:
        ...

    def task_done(self) -> None:
        ...


class CloseableQueueLike(QueueLike[_InputT, _OutputT], Protocol[_InputT, _OutputT]):

    # Note - these are implemented as properties in the concrete classes.
    # todo - remove the type: ignore comments when this is released:
    #  https://github.com/python/mypy/pull/13475
    is_closed: bool
    is_exhausted: bool

    def close(self) -> None:
        ...

    async def wait_closed(self) -> None:
        ...

    async def wait_exhausted(self) -> None:
        ...
