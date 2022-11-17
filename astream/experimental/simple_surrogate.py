from __future__ import annotations

from copy import copy
from dataclasses import dataclass, field
from functools import partialmethod, cached_property
from types import SimpleNamespace, CodeType
from typing import *

from executing import executing
from sorcery.core import FrameInfo

from astream import Stream, amap


class SurrogateOperation(NamedTuple):
    operation: str
    args: Tuple[Any, ...] = ()
    kwargs_: Dict[str, Any] | None = None

    @property
    def kwargs(self) -> Dict[str, Any]:
        if self.kwargs_ is None:
            return {}
        return self.kwargs_


class SimpleSurrogate:
    def __init__(self) -> None:
        self._surrogate_ops: list[SurrogateOperation] = []

    def __getattribute__(self, name: str) -> Callable[..., SimpleSurrogate]:
        print(f"__getattribute__({name})")
        if name.startswith(
            ("surrogate_", "_surrogate_", "issubclass", "isinstance", "__class", "__stream_map__")
        ):
            return super().__getattribute__(name)
        self._surrogate_ops.append(SurrogateOperation("__getattribute__", (name,)))
        return self

    def __call__(self, *args: Any, **kwargs: Any) -> SimpleSurrogate:
        self._surrogate_ops.append(SurrogateOperation("__call__", args, kwargs))
        return self

    #
    # def __getitem__(self, key: Any) -> SimpleSurrogate:
    #     self._surrogate_ops.append(SurrogateOperation("__getitem__", (key,)))
    #     return self

    @staticmethod
    def _surrogate_add_operation(
        name: str, flip_args: bool = False
    ) -> Callable[..., SimpleSurrogate]:
        def _add_operation(*args: Any, **kwargs: Any) -> SimpleSurrogate:
            self, *argsl = cast(SimpleSurrogate, args[0]), *args[1:]
            if flip_args:
                argsl = argsl[::-1]
            if any(isinstance(arg, SurrogateOperation) for arg in argsl) or any(
                isinstance(arg, SurrogateOperation) for arg in kwargs.values()
            ):
                raise ValueError("SurrogateOperation cannot be used as argument")
            self._surrogate_ops.append(SurrogateOperation(name, tuple(argsl), kwargs))
            return self

        return _add_operation

    __getitem__ = _surrogate_add_operation(name="__getitem__")
    __setitem__ = _surrogate_add_operation(name="__setitem__")
    __delitem__ = _surrogate_add_operation(name="__delitem__")
    __contains__ = _surrogate_add_operation(name="__contains__")
    __add__ = _surrogate_add_operation(name="__add__")
    __radd__ = _surrogate_add_operation(name="__radd__", flip_args=True)
    __sub__ = _surrogate_add_operation(name="__sub__")
    __rsub__ = _surrogate_add_operation(name="__rsub__", flip_args=True)
    __mul__ = _surrogate_add_operation(name="__mul__")
    __rmul__ = _surrogate_add_operation(name="__rmul__", flip_args=True)
    __matmul__ = _surrogate_add_operation(name="__matmul__")
    __rmatmul__ = _surrogate_add_operation(name="__rmatmul__", flip_args=True)
    __truediv__ = _surrogate_add_operation(name="__truediv__")
    __rtruediv__ = _surrogate_add_operation(name="__rtruediv__", flip_args=True)
    __floordiv__ = _surrogate_add_operation(name="__floordiv__")
    __rfloordiv__ = _surrogate_add_operation(name="__rfloordiv__", flip_args=True)
    __mod__ = _surrogate_add_operation(name="__mod__")
    __rmod__ = _surrogate_add_operation(name="__rmod__", flip_args=True)
    __divmod__ = _surrogate_add_operation(name="__divmod__")
    __rdivmod__ = _surrogate_add_operation(name="__rdivmod__", flip_args=True)
    __pow__ = _surrogate_add_operation(name="__pow__")
    __rpow__ = _surrogate_add_operation(name="__rpow__", flip_args=True)
    __lshift__ = _surrogate_add_operation(name="__lshift__")
    __rshift__ = _surrogate_add_operation(name="__rshift__")
    __rrshift__ = _surrogate_add_operation(name="__rrshift__", flip_args=True)
    __rlshift__ = _surrogate_add_operation(name="__rlshift__", flip_args=True)
    __and__ = _surrogate_add_operation(name="__and__")
    __rand__ = _surrogate_add_operation(name="__rand__", flip_args=True)
    __or__ = _surrogate_add_operation(name="__or__")
    __ror__ = _surrogate_add_operation(name="__ror__", flip_args=True)
    __xor__ = _surrogate_add_operation(name="__xor__")
    __rxor__ = _surrogate_add_operation(name="__rxor__", flip_args=True)
    __iadd__ = _surrogate_add_operation(name="__iadd__")
    __isub__ = _surrogate_add_operation(name="__isub__")
    __imul__ = _surrogate_add_operation(name="__imul__")
    __imatmul__ = _surrogate_add_operation(name="__imatmul__")
    __itruediv__ = _surrogate_add_operation(name="__itruediv__")
    __ifloordiv__ = _surrogate_add_operation(name="__ifloordiv__")
    __imod__ = _surrogate_add_operation(name="__imod__")
    __ipow__ = _surrogate_add_operation(name="__ipow__")
    __ilshift__ = _surrogate_add_operation(name="__ilshift__")
    __irshift__ = _surrogate_add_operation(name="__irshift__")
    __iand__ = _surrogate_add_operation(name="__iand__")
    __ior__ = _surrogate_add_operation(name="__ior__")
    __ixor__ = _surrogate_add_operation(name="__ixor__")
    __neg__ = _surrogate_add_operation(name="__neg__")
    __pos__ = _surrogate_add_operation(name="__pos__")
    __abs__ = _surrogate_add_operation(name="__abs__")
    __invert__ = _surrogate_add_operation(name="__invert__")
    __complex__ = _surrogate_add_operation(name="__complex__")
    __int__ = _surrogate_add_operation(name="__int__")
    __float__ = _surrogate_add_operation(name="__float__")
    __round__ = _surrogate_add_operation(name="__round__")
    __index__ = _surrogate_add_operation(name="__index__")
    __trunc__ = _surrogate_add_operation(name="__trunc__")
    __floor__ = _surrogate_add_operation(name="__floor__")
    __ceil__ = _surrogate_add_operation(name="__ceil__")
    __enter__ = _surrogate_add_operation(name="__enter__")
    __exit__ = _surrogate_add_operation(name="__exit__")
    __await__ = _surrogate_add_operation(name="__await__")
    __aiter__ = _surrogate_add_operation(name="__aiter__")
    __anext__ = _surrogate_add_operation(name="__anext__")
    __aenter__ = _surrogate_add_operation(name="__aenter__")
    __aexit__ = _surrogate_add_operation(name="__aexit__")
    __len__ = _surrogate_add_operation(name="__len__")
    __iter__ = _surrogate_add_operation(name="__iter__")

    __eq__ = _surrogate_add_operation(name="__eq__")
    __ne__ = _surrogate_add_operation(name="__ne__")
    __lt__ = _surrogate_add_operation(name="__lt__")
    __le__ = _surrogate_add_operation(name="__le__")
    __gt__ = _surrogate_add_operation(name="__gt__")
    __ge__ = _surrogate_add_operation(name="__ge__")

    def __bool__(self):
        self._surrogate_ops.append(SurrogateOperation("__bool__", (), {}))
        return True

    def surrogate_operations(self) -> tuple[SurrogateOperation, ...]:
        """Return the operations that have been added to this surrogate."""
        return tuple(self._surrogate_ops)

    def surrogate_clear_operations(self) -> None:
        """Clear all operations from this surrogate."""
        self._surrogate_ops.clear()

    def surrogate_get_partial(self) -> Callable[..., Any]:
        """Return a partial function that will call the same operations as performed on the
        surrogate since clearing.
        """
        ops = self.surrogate_operations()

        def _partial(target: Any) -> Any:
            for op in ops:
                target = getattr(target, op.operation)(*op.args, **op.kwargs)
            return target

        return _partial

    async def __stream_map__(self, stream: Stream) -> Stream:
        return stream.amap(self.surrogate_get_partial())  # todo - fix


import asyncio
import inspect
from types import SimpleNamespace

from typing import *

P = ParamSpec("P")
P2 = ParamSpec("P2")
T = TypeVar("T")
U = TypeVar("U")
R = TypeVar("R")

# CoroT = TypeVar("CoroT", bound=Coroutine[Any, Any, Any])
# CoroT = TypeVar("CoroT", bound=Coroutine[Any, Any, Any])
T_contra = TypeVar("T_contra", contravariant=True)
T_co = TypeVar("T_co", covariant=True)
CoroT: TypeAlias = Coroutine[Any, Any, T_co]
SyncOrAsyncResult: TypeAlias = CoroT[T_co] | T_co


@overload
def takes_sync_or_async(fn: Callable[P, CoroT[T]]) -> Callable[P, CoroT[T]]:
    ...


@overload
def takes_sync_or_async(fn: Callable[P, T]) -> Callable[P, CoroT[T]]:
    ...


def takes_sync_or_async(
    fn: Callable[P, Coroutine[Any, Any, T]] | Callable[P, T]
) -> Callable[P, Coroutine[Any, Any, T]]:
    if inspect.iscoroutinefunction(fn):
        return fn
    else:
        _fn = cast(Callable[P, T], fn)

        async def _async_fn(*args: P.args, **kwargs: P.kwargs) -> T:
            return _fn(*args, **kwargs)

        return _async_fn


async def async_fn(a: str, b: int) -> float:
    print("async fn")
    return float(a) * b


def sync_fn(a: int, b: str) -> bytes:
    print("sync fn")
    return b.encode() * a


async def main() -> None:
    async_fn_ = takes_sync_or_async(async_fn)
    sync_fn_ = takes_sync_or_async(sync_fn)
    print(await async_fn_("1", 2))
    print(await sync_fn_(2, "a"))


from sorcery import spell




class SorcerySurrogate:
    def __init__(self, expr="") -> None:
        self._expr = expr

    @spell
    def __call__(self, frame_info, *args, **kwargs):
        print("call", frame_info, args, kwargs, self.id)
        locs = locals() | frame_info.executing.frame.f_locals
        globs = globals() | frame_info.executing.frame.f_globals
        sentinel = object()

        if args or kwargs:
            x = kwargs.get("it", sentinel)
            if x is sentinel:
                x = args[0]
        else:
            x = ()

        locs["it"] = x

        if self._expr == "":
            return x

        # print(f"{self._expr=}")
        # return eval(self._expr, globs, locs)
        # todo - switch with `compile` and `exec` for performance
        # todo - try to cache accessing frame_info at all
        return eval(self._compiled_expr, globs, locs)

    @cached_property
    def _compiled_expr(self) -> CodeType:
        return compile(self._expr, "<string>", "eval")

    @spell
    def _do_get_expr_spell(self, frame_info: FrameInfo, *args, **kwargs):
        expr = self._get_expr(frame_info)
        return SorcerySurrogate(expr=expr)

    def _get_expr(self, frame_info: FrameInfo) -> str:
        call = frame_info.call
        line_no, end_lineno, col_offset, end_col_offset = (
            call.lineno,
            call.end_lineno,
            call.col_offset,
            call.end_col_offset,
        )
        if line_no == end_lineno:
            line = frame_info.executing.source.lines[line_no - 1]
            expr = line[col_offset:end_col_offset]
        else:
            raise NotImplementedError
        assert isinstance(expr, str)
        return expr

    @spell  # type: ignore
    def __getattr__(self, frame_info: FrameInfo, item: str) -> Any:
        if item in ("__qualname__", "__name__", "__annotations__"):
            return super().__getattribute__(item)

        return SorcerySurrogate(expr=self._get_expr(frame_info))
    @spell  # type: ignore
    def __setattr__(self, frame_info: FrameInfo, item: str, value: Any) -> Any:
        # print("setattr", item, value)
        if item in ("_expr",):
            return super().__setattr__(item, value)

        return SorcerySurrogate(expr=self._get_expr(frame_info))

    def __hash__(self) -> int:
        return hash(id(self))

    # async def __stream_map__(self, stream: Stream[Any]) -> Stream[Any]:
    #     print("stream map", self._expr, stream)
    #     return amap(self, stream)  # todo - fix

    __getitem__ = _do_get_expr_spell
    __setitem__ = _do_get_expr_spell
    __delitem__ = _do_get_expr_spell
    __add__ = _do_get_expr_spell
    __radd__ = _do_get_expr_spell
    __sub__ = _do_get_expr_spell
    __rsub__ = _do_get_expr_spell
    __mul__ = _do_get_expr_spell
    __rmul__ = _do_get_expr_spell
    __matmul__ = _do_get_expr_spell
    __rmatmul__ = _do_get_expr_spell
    __truediv__ = _do_get_expr_spell
    __rtruediv__ = _do_get_expr_spell
    __floordiv__ = _do_get_expr_spell
    __rfloordiv__ = _do_get_expr_spell
    __mod__ = _do_get_expr_spell
    __rmod__ = _do_get_expr_spell
    __divmod__ = _do_get_expr_spell
    __rdivmod__ = _do_get_expr_spell
    __pow__ = _do_get_expr_spell
    __rpow__ = _do_get_expr_spell
    __lshift__ = _do_get_expr_spell
    __rshift__ = _do_get_expr_spell
    __rrshift__ = _do_get_expr_spell
    __rlshift__ = _do_get_expr_spell
    __and__ = _do_get_expr_spell
    __rand__ = _do_get_expr_spell
    __or__ = _do_get_expr_spell
    __ror__ = _do_get_expr_spell
    __xor__ = _do_get_expr_spell
    __rxor__ = _do_get_expr_spell
    __iadd__ = _do_get_expr_spell
    __isub__ = _do_get_expr_spell
    __imul__ = _do_get_expr_spell
    __imatmul__ = _do_get_expr_spell
    __itruediv__ = _do_get_expr_spell
    __ifloordiv__ = _do_get_expr_spell
    __imod__ = _do_get_expr_spell
    __ipow__ = _do_get_expr_spell
    __ilshift__ = _do_get_expr_spell
    __irshift__ = _do_get_expr_spell
    __iand__ = _do_get_expr_spell
    __ior__ = _do_get_expr_spell
    __ixor__ = _do_get_expr_spell
    __neg__ = _do_get_expr_spell
    __pos__ = _do_get_expr_spell
    __abs__ = _do_get_expr_spell
    __invert__ = _do_get_expr_spell
    __complex__ = _do_get_expr_spell
    __int__ = _do_get_expr_spell
    __float__ = _do_get_expr_spell
    __round__ = _do_get_expr_spell
    __trunc__ = _do_get_expr_spell
    __floor__ = _do_get_expr_spell
    __ceil__ = _do_get_expr_spell
    __enter__ = _do_get_expr_spell
    __exit__ = _do_get_expr_spell
    __await__ = _do_get_expr_spell
    __aiter__ = _do_get_expr_spell
    __anext__ = _do_get_expr_spell
    __aenter__ = _do_get_expr_spell
    __aexit__ = _do_get_expr_spell
    __len__ = _do_get_expr_spell
    __iter__ = _do_get_expr_spell
    __eq__ = _do_get_expr_spell
    __ne__ = _do_get_expr_spell
    __lt__ = _do_get_expr_spell
    __le__ = _do_get_expr_spell
    __gt__ = _do_get_expr_spell
    __ge__ = _do_get_expr_spell

    __contains__ = _do_get_expr_spell
    __missing__ = _do_get_expr_spell

    # __index__ = lambda *args, **kwargs: 1
    # __str__ = lambda *args, **kwargs: "1"



it = SorcerySurrogate()
f = 1 + 2
abc = 1 + it + 1 * 4
n = 100

dd = it[2].something

target = [12, 34, SimpleNamespace(some=3, something=47)]

print(f"val is {dd(target)}")

from astream import stream, apredicate_map, arange

d = ({"abc": [0, SimpleNamespace(some=3, something=i, other={"cde": i ** 7})]} for i in range(20))

async def main() -> None:
    async for x in stream(d) / apredicate_map(
        {
            it["abc"][1].something % 2 == 0: it["abc"][1].something,
            it["abc"][1].something % 3 == 0: it["abc"][1].other["cde"],
            (it["abc"]): it["abc"][1].something,
        }
    ) / (lambda ax: ax + 2):
        print(x)


asyncio.run(main())

abc(6)
y = abc(5)
print(y)
