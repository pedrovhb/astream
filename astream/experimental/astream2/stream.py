from __future__ import annotations

import asyncio
import inspect
import sys
from collections import deque
from collections.abc import AsyncIterator, Callable, Iterable
from functools import wraps, singledispatchmethod
from itertools import count
from typing import (
    TypeVar,
    ParamSpec,
    TypeAlias,
    Coroutine,
    AsyncIterable,
    Concatenate,
    cast,
    TYPE_CHECKING,
    Protocol,
    runtime_checkable,
    Generator,
    Awaitable,
    overload,
)

from asynkets import ensure_coroutine_function, ensure_async_iterator


_T = TypeVar("_T", covariant=True)
_U = TypeVar("_U")
_P = ParamSpec("_P")

_T_co = TypeVar("_T_co", covariant=True)
_T_contra = TypeVar("_T_contra", contravariant=True)

AnyFunction: TypeAlias = Callable[_P, _T_co] | Callable[_P, Coroutine[object, object, _T_co]]
AnyIterable: TypeAlias = AsyncIterable[_T_co] | Iterable[_T_co]

TransformerIterable: TypeAlias = Callable[[AsyncIterator[_T]], AsyncIterator[_U]]

_SourceT = TypeVar("_SourceT", contravariant=True)
_ResultT = TypeVar("_ResultT", covariant=True)

_AwaitableT = TypeVar("_AwaitableT", bound=Awaitable)


class _NoValueT:
    pass


_NoValue = _NoValueT()


class Stream(AsyncIterator[_T], AsyncIterable[_T]):
    def __init__(self, it: AnyIterable[_T]):
        self._async_iterator = ensure_async_iterator(it)

        self._previous_item: _T | _NoValueT = _NoValue

    async def collect(self) -> _T:
        item = _NoValue
        async for item in self:
            pass
        if isinstance(item, _NoValueT):
            raise ValueError("Stream is empty")
        return item

    async def __anext__(self) -> _T:
        return await anext(self._async_iterator)

    def __aiter__(self) -> AsyncIterator[_T]:
        return self

    def __await__(self) -> Generator[None, None, _T]:
        return self.collect().__await__()

    def map(
        self,
        func: AnyFunction[[_T], _U]
        | Callable[[_T], _U]
        | TransformerIterable[_T, _U]
        | Callable[[], TransformerIterable[_T, _U]],
    ) -> Stream[_U]:

        # Todo - lambdas. They work in the following context:
        # from typing import Callable, TypeVar, overload
        #
        # l = lambda x: str(x * 2)
        #
        # T = TypeVar("T")
        # U = TypeVar("U")
        #
        #
        # @overload
        # def applier(func: Callable[[T], U], arg: T) -> U:
        #     ...
        #
        #
        # @overload
        # def applier(func: Callable[[T], T], arg: T) -> T:
        #     ...
        #
        #
        # def applier(fn, val: T):
        #     return fn(val)
        #
        #
        # x = 5
        # reveal_type(applier(l, x))
        # reveal_type(applier(lambda x: x * 2, x))

        if inspect.isasyncgenfunction(func):
            # Handle transformer functions - functions that take an async
            # iterator and return an async iterator.
            _transformer_func = cast(TransformerIterable[_T, _U], func)
            return Stream(_transformer_func(self))

        elif hasattr(func, "is_transformer") and func.is_transformer:
            # Handle transformer functions which are not async generators
            # because they haven't been called - this can happen normally
            # when they have default arguments only.
            try:
                _transformer_func = cast(Callable[[], TransformerIterable[_T, _U]], func)
                return Stream(_transformer_func()(self))

            except TypeError as e:
                # If the function is missing required arguments, we can't
                # call it, so we can't use it as a transformer.
                raise TypeError(
                    "Transformer function must be called with all required arguments"
                ) from e

        elif callable(func):
            # Handle normal functions. These will be mapped over the stream,
            # being applied to each item.

            async def _mapped() -> AsyncIterator[_U]:
                _func = ensure_coroutine_function(func)
                async for item in self:
                    yield await _func(item)

            return Stream(_mapped())

        else:
            return NotImplemented

    def filter(self, func: AnyFunction[[_T], bool]) -> Stream[_T]:
        async def _filtered() -> AsyncIterator[_T]:
            _func = ensure_coroutine_function(func)
            async for item in self:
                if await _func(item):
                    yield item

        return Stream(_filtered())

    def flat_map(
        self,
        func: AnyFunction[[_T], AnyIterable[_U]] | TransformerIterable[_T, AnyIterable[_U]],
    ) -> Stream[_U]:
        async def _flat_mapped() -> AsyncIterator[_U]:
            _func = ensure_coroutine_function(func)
            async for item in self:
                mapped = await _func(item)
                async for mapped_item in ensure_async_iterator(mapped):
                    yield mapped_item

        return Stream(_flat_mapped())

    def flatten(self: Stream[AnyIterable[_U]]) -> Stream[_U]:
        async def _flattened() -> AsyncIterator[_U]:
            async for item in self:
                print("item", item)
                async for sub_item in ensure_async_iterator(item):
                    yield sub_item

        return Stream(_flattened())

    def sink_to(
        self,
        sink_fn: Callable[[AsyncIterator[_T]], _U]
        | Callable[[], Callable[[AsyncIterator[_T]], _U]],
    ) -> _U:

        if hasattr(sink_fn, "is_sink") and sink_fn.is_sink:
            # Handle sink functions which are not async generators
            # because they haven't been called - this can happen normally
            # when they have default arguments only.
            try:
                _partial_sink_fn = cast(
                    Callable[[], Callable[[AsyncIterator[_T]], _U]],
                    sink_fn,
                )
                # _async_sink_fn = ensure_coroutine_function(_partial_sink_fn())
                return _partial_sink_fn()(self)

            except TypeError as e:
                # If the function is missing required arguments, we can't
                # call it, so we can't use it as a sink.
                raise TypeError("Sink function must be called with all required arguments") from e

        # _async_sink_fn = ensure_coroutine_function(sink_fn)
        _partial_sink_fn = cast(Callable[[AsyncIterator[_T]], _U], sink_fn)
        return _partial_sink_fn(self)

    @overload
    def __getitem__(self, index: int) -> Coroutine[object, object, _T]:
        ...

    @overload
    def __getitem__(self, index: slice) -> Stream[_T]:
        ...

    def __getitem__(self, index: int | slice) -> Coroutine[object, object, _T] | "Stream[_T]":

        if isinstance(index, int):

            if index >= 0:

                async def _at_index(stream: Stream[_U], target_index: int) -> _U:
                    i = 0
                    async for item in stream:
                        if i == target_index:
                            return item
                        i += 1
                    raise IndexError(f"Index {target_index} out of range")

                return _at_index(self, index)
            else:

                async def _at_negative_index(stream: Stream[_U], target_index: int) -> _U:
                    items = deque(maxlen=-target_index)
                    async for item in stream:
                        items.append(item)
                    if len(items) < -target_index:
                        raise IndexError(f"Index {target_index} out of range")
                    return items[0]

                return _at_negative_index(self, index)

        elif isinstance(index, slice):
            if index.step is not None and index.step < 0:
                raise ValueError("Slice step cannot be negative")

            async def _slice(stream: Stream[_U], target_slice: slice) -> AsyncIterator[_U]:
                i = 0
                counter = range(
                    target_slice.start or 0,
                    target_slice.stop or sys.maxsize,
                    target_slice.step or 1,
                )
                async for item in stream:
                    if i in counter:
                        yield item
                    i += 1

            return Stream(_slice(self, index))
        else:
            raise TypeError(f"Invalid index type: {type(index)}")

    # Define operator overloads for Stream.
    # They're mapped as follows:
    #   stream / func       -> stream.map(func)
    #   stream % func       -> stream.filter(func)
    #   stream // func      -> stream.flat_map(func)
    #   +stream             -> stream.flatten()

    __mod__ = filter
    __floordiv__ = flat_map
    __matmul__ = sink_to

    __pos__ = flatten

    def __truediv__(
        self,
        func: AnyFunction[[_T], _U]
        | Callable[[_T], _U]
        | TransformerIterable[_T, _U]
        | Callable[[], TransformerIterable[_T, _U]],
    ) -> Stream[_U]:
        return self.map(func)


def transformer(
    func: Callable[Concatenate[AsyncIterator[_T], _P], AsyncIterator[_U]]
) -> Callable[_P, Callable[[AsyncIterable[_T]], AsyncIterator[_U]]]:
    """Decorator to create a transformer function from a function that takes
    an AsyncIterator as its first argument.

    Effectively, this allows you to "partially apply" arguments from an
    async iterable -> async iterable function which takes an async iterable plus
    some arguments, transforming it into a function that takes just an async iterable
    and returns another one.
    """

    def _transformer(
        *__args: _P.args, **__kwargs: _P.kwargs
    ) -> Callable[[AsyncIterable[_T]], AsyncIterator[_U]]:
        async def _transformer_func(__stream: AsyncIterable[_T]) -> AsyncIterator[_U]:

            # We can loosen up here by calling aiter on the first argument -
            # that way, the function we take can accept only AsyncIterators,
            # but the function we return can accept any AsyncIterable.
            async for item in func(aiter(__stream), *__args, **__kwargs):
                yield item

        return _transformer_func

    _transformer.is_transformer = True
    _transformer.wrapped_func = func

    # todo - fix typing for this, see -
    #  Stream() / aenumerate <- wrong
    #  Stream() / aenumerate() <- right
    #  Stream() / aenumerate(1) <- wrong

    return _transformer


Partializer: TypeAlias = Callable[
    [Callable[Concatenate[_T, _P], _U]],
    Callable[_P, Callable[[_T], _U]],
]


def sink(
    func: Callable[Concatenate[AsyncIterator[_T], _P], _U]
) -> Callable[_P, Callable[[AsyncIterator[_T]], _U]]:
    """Decorator to create a sink function from a function that takes
    an AsyncIterator as its first argument.

    Effectively, this allows you to "partially apply" arguments from an
    async iterable -> async iterable function which takes an async iterable plus
    some arguments, transforming it into a function that takes just an async iterable
    and returns another one.
    """

    @wraps(func)
    def _sink(*__args: _P.args, **__kwargs: _P.kwargs) -> Callable[[AsyncIterator[_T]], _U]:
        def _sink_func(__stream: AsyncIterator[_T]) -> _U:
            return func(aiter(__stream), *__args, **__kwargs)

        return _sink_func

    _sink.is_sink = True
    _sink.wrapped_func = func
    return _sink


if __name__ == "__main__":

    async def main() -> None:
        def mul_2(x: int) -> int:
            return x * 2

        def is_div_4(x: int) -> bool:
            return x % 4 == 0

        def spelled_out(x: int) -> Iterable[str]:
            nums = {
                "1": "one",
                "2": "two",
                "3": "three",
                "4": "four",
                "5": "five",
                "6": "six",
                "7": "seven",
                "8": "eight",
                "9": "nine",
                "0": "zero",
            }
            for num in str(x):
                yield nums[num]

        a = Stream(range(100)) / mul_2 % is_div_4
        # reveal_type(a)
        b = a // spelled_out
        # reveal_type(b)
        from astream.streamtools import pairwise, nwise, aenumerate, batched

        c = b / pairwise / nwise(3) / aenumerate
        # reveal_type(c)

        # c = b / pairwise / nwise(3) / aenumerate / nwise
        # The above correctly fails type check (nwise has required parameters)

        # print(await (c @ count_elements))
        d = await c
        print(d)
        # async for item in c:
        #     print(item)
        #     if TYPE_CHECKING:
        #         reveal_type(item)
        # information: Type of "item" is "tuple[int, tuple[tuple[str, str], ...]]"

        # (0, (('zero', 'four'), ('four', 'eight'), ('eight', 'one')))
        # (1, (('four', 'eight'), ('eight', 'one'), ('one', 'two')))
        # (2, (('eight', 'one'), ('one', 'two'), ('two', 'one')))
        # (3, (('one', 'two'), ('two', 'one'), ('one', 'six')))

        t_batch = Stream(range(100)) / batched(11) / sum
        # reveal_type(t_batch)

        async def summy(x: AsyncIterator[int]) -> int:
            return sum([xx async for xx in x])

        @sink
        async def summy_params(x: AsyncIterator[int], abc: int = 10) -> int:
            return sum([xx async for xx in x]) + abc

        total = await (t_batch @ summy_params(3))
        print(total)

        async for item in Stream(range(100))[:50:6]:
            print(item)

        last = await Stream(range(100))[99]
        print(last)

        last = await Stream(range(100))[-100]
        print(last)

        reveal_type(Stream(range(100)).map(mul_2))
        reveal_type(Stream(range(100)).map(lambda x: x * 2))
        reveal_type(Stream(range(100)) / (lambda x: x * 2))

        # async for item in t_batch:
        #     print(item)
        #     if TYPE_CHECKING:
        #         reveal_type(item)

    asyncio.run(main())


__all__ = ("sink", "Stream", "transformer")
