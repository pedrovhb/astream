from __future__ import annotations

from typing import (
    TypeVar,
    TypeAlias,
    Coroutine,
    Any,
    ParamSpec,
    NewType, Protocol, Union, Callable,
)

InT = TypeVar("InT")
OutT = TypeVar("OutT")

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)
T_contra = TypeVar("T_contra", contravariant=True)
R = TypeVar("R")

T_co_B = TypeVar("T_co_B", covariant=True)
T_contra_B = TypeVar("T_contra_B", contravariant=True)

P = ParamSpec("P")
P2 = ParamSpec("P2")

CoroT: TypeAlias = Coroutine[Any, Any, T]


# Define protocols instead of type aliases; this is a workaround for the mypy issue below,
# which prevents ParamSpec from being used in type aliases.
# https://github.com/python/mypy/issues/11855
# (actually, still not sure even protocol is a workaround)
# class CoroFnT(Protocol[P, T_co]):
#     async def __call__(self, *__args: P.args, **__kwargs: P.kwargs) -> T_co: ...
#
#
# class FnT(Protocol[P, T_co]):
#     def __call__(self, *__args: P.args, **__kwargs: P.kwargs) -> T_co: ...
#
#
class UnaryAsyncFnT(Protocol[T_contra, T_co]):
    async def __call__(self, __value: T_contra) -> T_co: ...


class UnarySyncFnT(Protocol[T_contra_B, T_co_B]):
    def __call__(self, __value: T_contra_B) -> T_co_B: ...


class SentinelType:
    pass


_NoValueSentinelT = NewType("_NoValueSentinelT", SentinelType)
NoValueSentinel = _NoValueSentinelT(SentinelType())  # noqa
