from __future__ import annotations

import asyncio
from itertools import chain
from typing import (
    Any,
    AsyncIterable,
    Callable,
    Coroutine,
    Iterable,
    TypeAlias,
    TypeVar,
    cast,
)

from .utils import NoValue, SentinelType, ensure_async_iterator, ensure_coro_fn

_T = TypeVar("_T")
_U = TypeVar("_U")
_CoroT: TypeAlias = Coroutine[Any, Any, _T]


async def areduce(
    fn: Callable[[_T, _U], _CoroT[_T]] | Callable[[_T, _U], _T],
    iterable: AsyncIterable[_U],
    initial: _T | SentinelType = NoValue,
) -> _T:
    """An asynchronous version of `reduce`.

    Args:
        fn: The function to reduce with.
        iterable: The iterable to reduce.
        initial: The initial value to reduce with.

    Returns:
        The reduced value.

    Examples:
        >>> from astream import arange
        >>> async def demo_areduce():
        ...     print(await areduce(lambda a, b: a + b, arange(5)))
        >>> asyncio.run(demo_areduce())
        10

        >>> async def demo_areduce():
        ...     print(await areduce(lambda a, b: a + b, arange(5), 5))
        >>> asyncio.run(demo_areduce())
        15
    """
    _fn_async = ensure_coro_fn(fn)
    _it_async = ensure_async_iterator(iterable)

    if initial is NoValue:
        initial = await anext(_it_async)  # type: ignore
    crt = cast(_T, initial)

    async for item in _it_async:
        crt = await _fn_async(crt, item)  # type: ignore
    return crt


NoData = object()


def dotget(path: str) -> Callable[[Any], Iterable[Any]]:
    def _adotget(p: str, *objs: Any) -> Any:
        if not p:
            yield from objs

        part, parts = p.split(".", maxsplit=1) if "." in p else (p, "")

        matches = []
        for obj in objs:

            match obj:
                case dict() if part in obj:
                    matches.append(_adotget(parts, obj[part]))
                case list() if part.isdigit() and int(part) < len(obj):
                    matches.append(_adotget(parts, obj[int(part)]))
                case dict() if part == "*":
                    for v in obj.values():
                        matches.append(_adotget(parts, v))
                case list() if part == "*":
                    for v in obj:
                        matches.append(_adotget(parts, v))
                case _ if (result := getattr(obj, part, NoData)) is not NoData:
                    matches.append(_adotget(parts, result))
                case _:
                    pass

        yield from chain.from_iterable(matches)

    def _call(*objs: Any) -> Any:
        return _adotget(path, *objs)

    return _call
