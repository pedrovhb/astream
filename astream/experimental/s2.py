from __future__ import annotations
from functools import wraps

import asyncio
from abc import abstractmethod
from collections import deque
from typing import *

T = TypeVar("T")
P = ParamSpec("P")

StreamT = TypeVar('StreamT', covariant=True)
InT = TypeVar('InT', contravariant=True)
OutT = TypeVar('OutT', covariant=True)





class TransformerFnProtocol(Protocol[InT, OutT, P]):

    async def __call__(self, __stream: AsyncIterator[InT], *__args: P.args,  **__kwargs: P.kwargs) -> AsyncIterator[OutT]:
        ...

class TransformerProtocol(Protocol[InT, OutT]):
    def __call__(self, stream: AsyncIterable[InT]) -> Stream[OutT]:
        ...


class Stream(Generic[StreamT], AsyncIterable[StreamT]):


    # @overload
    # def __init__(self, stream: Iterable[StreamT]) -> None: ...
    #
    # @overload
    # def __init__(self, stream: AsyncIterable[StreamT]) -> None: ...

    def __init__(self, stream: AsyncIterable[StreamT]) -> None:
        # if isinstance(stream, AsyncIterable):
        #     self._items = aiter(stream)
        # elif isinstance(_its := stream, Iterable):
        #     async def _iter() -> AsyncIterator[StreamT]:
        #         for item in _its:
        #             yield item
        #
        #     self._items = _iter()
        self._items = aiter(stream)

    def __aiter__(self) -> AsyncIterator[StreamT]:
        return self._items


def pairwise(stream: AsyncIterable[T]) -> Stream[tuple[T, T]]:
    async def _pairwise() -> AsyncIterator[tuple[T, T]]:
        async for item in stream:
            yield item, item

    return Stream(_pairwise())



def nwise(n: int) -> Callable[[AsyncIterable[InT]], Stream[Tuple[InT, ...]]]:
    def _nwise(async_iterable: AsyncIterable[InT]) -> Stream[Tuple[InT, ...]]:
        async def _iter() -> AsyncIterator[tuple[InT, ...]]:
            prev = deque[InT](maxlen=n)
            async for item in async_iterable:
                prev.append(item)
                if len(prev) == n:
                    yield tuple(prev)

        return Stream(_iter())

    return _nwise

InT2 = TypeVar('InT2', contravariant=True)
OutT2 = TypeVar('OutT2', covariant=True)

InT3 = TypeVar('InT3', contravariant=True)
T = TypeVar("T", covariant=True)
U = TypeVar("U", covariant=True)
V = TypeVar("V", covariant=True)


AsyncIteratorTransformerFn: TypeAlias = Callable[Concatenate[AsyncIterator[InT], P], AsyncIterator[OutT]]
TransformerFn: TypeAlias = Callable[P, Callable[[AsyncIterable[InT]], Stream[OutT]]]

def async_generator_decorator(async_generator_func: AsyncIteratorTransformerFn[InT, P, OutT]) -> TransformerFn[P, InT, OutT]:
    @wraps(async_generator_func)
    def wrapper(*__args: P.args, **__kwargs: P.kwargs) -> Callable[[AsyncIterable[InT]], Stream[OutT]]:
        def inner(__async_iterable: AsyncIterable[InT]) -> Stream[OutT]:
            async_iterator = aiter(__async_iterable)
            out = async_generator_func(async_iterator, *__args, **__kwargs)
            return Stream(out)
        return inner
    return wrapper


@async_generator_decorator
async def nwise2(async_iterator: AsyncIterator[InT], n: int) -> AsyncIterator[tuple[InT, ...]]:
    prev = deque[InT](maxlen=n)
    async for item in async_iterator:
        prev.append(item)
        if len(prev) == n:
            yield tuple(prev)



async def main() -> None:

    if not TYPE_CHECKING:
        def reveal_type(x: object) -> None:
            print(f"{x} is of type {type(x)}")

    async def arange(n: int) -> AsyncIterator[int]:
        for i in range(n):
            yield i

    reveal_type(nwise)
    reveal_type(nwise(3))
    reveal_type(nwise(3)(pairwise(arange(10))))

    reveal_type(nwise2)
    reveal_type(nwise2(3))  # This is a built function which is unbound
    reveal_type(nwise2(3)(pairwise(arange(10))))  # Can't bind here anymore


    async for item in nwise2(3)(Stream(arange(10))):
        print(item)
        reveal_type(item)  # pyright works

if __name__ == '__main__':
    asyncio.run(main())
