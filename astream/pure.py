"""Pure async iteration functions (i.e. not custom Stream/Transformer)."""

from __future__ import annotations

from typing import AsyncIterable, AsyncIterator, Iterable, overload, TypeVar

from .utils import ensure_async_iterator

_T = TypeVar("_T")


@overload
def aflatten(iterable: AsyncIterator[Iterable[_T]]) -> AsyncIterator[_T]:
    ...


@overload
def aflatten(iterable: AsyncIterator[AsyncIterable[_T]]) -> AsyncIterator[_T]:
    ...


async def aflatten(
    iterable: AsyncIterator[Iterable[_T]] | AsyncIterator[AsyncIterable[_T]],
) -> AsyncIterator[_T]:
    """Unpacks an async iterator of iterables or async iterables into a flat async iterator."""
    async for item in iterable:
        async for subitem in ensure_async_iterator(item):
            yield subitem


__all__ = ("aflatten",)
