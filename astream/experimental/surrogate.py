from __future__ import annotations

from collections import deque
from functools import partialmethod
from typing import Generic, TypeVar, Any, Callable, cast, overload

from astream import Stream, ensure_coro_fn

_T = TypeVar("_T")
_U = TypeVar("_U")


class SurrogateOperation:
    op: str
    op_args: tuple[Any, ...]
    op_kwargs: dict[str, Any]

    def __init__(self, op: str, *args: Any, **kwargs: Any) -> None:
        self.op = op
        self.op_args = args
        self.op_kwargs = kwargs

    def __str__(self) -> str:
        rep_args_str = ", ".join(repr(arg) for arg in self.op_args if not isinstance(arg, It))
        args_str = ", ".join(str(arg) for arg in self.op_args if not isinstance(arg, It))
        kwargs_str = ", ".join(
            f"{k!r}={v!r}"
            for k, v in self.op_kwargs.items()
            if not isinstance(v, It) and not isinstance(k, It)
        )
        if args_str and kwargs_str:
            args_str = f"{args_str}, {kwargs_str}"
        elif kwargs_str:
            args_str = kwargs_str

        op_str = {
            "__iter__": "(iter)",
            "__next__": "(next)",
            "__call__": f"({rep_args_str})",
            "__getitem__": f"[{args_str}]",
            "__add__": f" + {args_str}",
            "__sub__": f" - {args_str}",
            "__mul__": f" * {args_str}",
            "__truediv__": f" / {args_str}",
            "__floordiv__": f" // {args_str}",
            "__mod__": f" % {args_str}",
            "__pow__": f" ** {args_str}",
            "__lshift__": f" << {args_str}",
            "__rshift__": f" >> {args_str}",
            "__and__": f" & {args_str}",
            "__xor__": f" ^ {args_str}",
            "__or__": f" | {args_str}",
            "__matmul__": f" @ {args_str}",
            "__radd__": f"{args_str} + ",
            "__rsub__": f"{args_str} - ",
            "__rmul__": f"{args_str} * ",
            "__rtruediv__": f"{args_str} / ",
            "__rfloordiv__": f"{args_str} // ",
            "__rmod__": f"{args_str} % ",
            "__rpow__": f"{args_str} ** ",
            "__rlshift__": f"{args_str} << ",
            "__rrshift__": f"{args_str} >> ",
            "__rand__": f"{args_str} & ",
            "__rxor__": f"{args_str} ^ ",
            "__ror__": f"{args_str} | ",
            "__rmatmul__": f"{args_str} @ ",
            "__iadd__": f" += {args_str}",
            "__isub__": f" -= {args_str}",
            "__imul__": f" *= {args_str}",
            "__itruediv__": f" /= {args_str}",
            "__ifloordiv__": f" //= {args_str}",
            "__imod__": f" %= {args_str}",
            "__ipow__": f" **= {args_str}",
            "__ilshift__": f" <<= {args_str}",
            "__irshift__": f" >>= {args_str}",
            "__iand__": f" &= {args_str}",
            "__ixor__": f" ^= {args_str}",
            "__ior__": f" |= {args_str}",
            "__imatmul__": f" @= {args_str}",
            "__neg__": "",
            "__pos__": "",
            "__abs__": "",
            "__invert__": "",
            "__complex__": "",
            "__int__": "",
            "__float__": "",
            "__round__": "",
            "__index__": "",
            "__trunc__": "",
            "__floor__": "",
            "__ceil__": "",
            "__enter__": "",
            "__exit__": "",
            "__await__": "",
            "__aiter__": "",
            "__anext__": "",
            "__aenter__": "",
            "__aexit__": "",
            "__len__": "",
            "__bool__": "",
            "__bytes__": "",
            "__str__": "",
            "__repr__": "",
            "__format__": "",
            "__hash__": "",
            "__eq__": f" == {args_str}",
            "__ne__": f" != {args_str}",
            "__lt__": f" < {args_str}",
            "__le__": f" <= {args_str}",
            "__gt__": f" > {args_str}",
            "__ge__": f" >= {args_str}",
            "__contains__": f" in {args_str}",
            "__missing__": f" not in {args_str}",
            "__get__": f"",
            "__set__": f"",
            "__delete__": f"del {args_str}",
            "__getattribute__": f".{args_str}",
            "__setattr__": ".{args_str} = {value!r}",
            "__delattr__": f"del {args_str}",
            "__dir__": "",
            "__sizeof__": "",
            "__reversed__": "",
            "__copy__": "",
            "__deepcopy__": "",
            "__reduce__": "",
            "__reduce_ex__": "",
            "__getnewargs_ex__": "",
            "__getnewargs__": "",
            "__setstate__": "",
            "__getstate__": "",
            "__del__": "",
            "__init__": "",
            "__new__": "",
            "__init_subclass__": "",
            "__subclasshook__": "",
            "__instancecheck__": "",
            "__classcheck__": "",
            "__prepare__": "",
            "__instancehook__": "",
            "__classhook__": "",
        }[self.op]

        return op_str

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.op!r}, {self.op_args!r}, {self.op_kwargs!r})"


class It(Generic[_T, _U]):
    def __init__(self) -> None:
        self._it_ops = deque[SurrogateOperation]()

    def __getitem__(self, *args: Any, **kwargs: Any) -> It[_T, _U]:
        self._it_ops.append(SurrogateOperation("__getitem__", *args, **kwargs))
        return self

    def __getattribute__(self, *args: Any, **kwargs: Any) -> It[_T, _U]:
        if args[0] in ("_it_ops", "it_make_fn", "__orig_class__", "__class__"):
            return object.__getattribute__(self, *args, **kwargs)  # type: ignore
        self._it_ops.append(SurrogateOperation("__getattribute__", *args, **kwargs))
        return self

    def __setattr__(self, *args: Any, **kwargs: Any) -> None:
        if args[0] in ("_it_ops", "it_make_fn", "__orig_class__", "__name__"):
            return super().__setattr__(*args, **kwargs)
        else:
            self._it_ops.append(SurrogateOperation("__setattr__", *args, **kwargs))

    def __setitem__(self, *args: Any, **kwargs: Any) -> None:
        self._it_ops.append(SurrogateOperation("__setitem__", *args, **kwargs))

    def __register_action__(self, method: str, *args: Any, **kwargs: Any) -> It[_T, _U]:
        self._it_ops.append(SurrogateOperation(method, *args, **kwargs))
        return self

    __call__ = partialmethod(__register_action__, "__call__")
    __add__ = partialmethod(__register_action__, "__add__")
    __sub__ = partialmethod(__register_action__, "__sub__")
    __mul__ = partialmethod(__register_action__, "__mul__")
    __truediv__ = partialmethod(__register_action__, "__truediv__")
    __floordiv__ = partialmethod(__register_action__, "__floordiv__")
    __mod__ = partialmethod(__register_action__, "__mod__")
    __pow__ = partialmethod(__register_action__, "__pow__")
    __lshift__ = partialmethod(__register_action__, "__lshift__")
    __rshift__ = partialmethod(__register_action__, "__rshift__")
    __and__ = partialmethod(__register_action__, "__and__")
    __xor__ = partialmethod(__register_action__, "__xor__")
    __or__ = partialmethod(__register_action__, "__or__")
    __matmul__ = partialmethod(__register_action__, "__matmul__")
    __radd__ = partialmethod(__register_action__, "__radd__")
    __rsub__ = partialmethod(__register_action__, "__rsub__")
    __rmul__ = partialmethod(__register_action__, "__rmul__")
    # __rmod__ = partialmethod(__register_action__, "__rmod__")
    __rpow__ = partialmethod(__register_action__, "__rpow__")
    __rlshift__ = partialmethod(__register_action__, "__rlshift__")
    __rrshift__ = partialmethod(__register_action__, "__rrshift__")
    __rand__ = partialmethod(__register_action__, "__rand__")
    __rxor__ = partialmethod(__register_action__, "__rxor__")
    __ror__ = partialmethod(__register_action__, "__ror__")
    __rmatmul__ = partialmethod(__register_action__, "__rmatmul__")
    __iadd__ = partialmethod(__register_action__, "__iadd__")
    __isub__ = partialmethod(__register_action__, "__isub__")
    __imul__ = partialmethod(__register_action__, "__imul__")
    __itruediv__ = partialmethod(__register_action__, "__itruediv__")
    __ifloordiv__ = partialmethod(__register_action__, "__ifloordiv__")
    __imod__ = partialmethod(__register_action__, "__imod__")
    __ipow__ = partialmethod(__register_action__, "__ipow__")
    __ilshift__ = partialmethod(__register_action__, "__ilshift__")
    __irshift__ = partialmethod(__register_action__, "__irshift__")
    __iand__ = partialmethod(__register_action__, "__iand__")
    __ixor__ = partialmethod(__register_action__, "__ixor__")
    __ior__ = partialmethod(__register_action__, "__ior__")
    __imatmul__ = partialmethod(__register_action__, "__imatmul__")
    __neg__ = partialmethod(__register_action__, "__neg__")
    __pos__ = partialmethod(__register_action__, "__pos__")

    def __str__(self) -> str:
        return f"<It {''.join(str(o) for o in self._it_ops)}>"

    def __repr__(self) -> str:
        return f"<It {''.join(str(o) for o in self._it_ops)}>"

    def it_make_fn(self) -> Callable[[_T], _U]:
        def fn(_obj: _T) -> _U:
            _modded_obj = _obj
            for op in self._it_ops:
                _modded_obj = getattr(_modded_obj, op.op)(*op.op_args, **op.op_kwargs)
            return cast(_U, _modded_obj)

        return fn

    @overload
    def __rtruediv__(self, other: Stream[_T]) -> Stream[_U]:
        ...

    @overload
    def __rtruediv__(self, other: Any) -> Stream[_U] | It[_T, _U]:
        ...

    def __rtruediv__(self, other: Any) -> Stream[_U] | It[_T, _U]:
        if isinstance(other, Stream):
            return other.amap(self.it_make_fn())
        self._it_ops.append(SurrogateOperation("__rtruediv__", (other,)))
        return self

    @overload
    def __rfloordiv__(self, other: Stream[_T]) -> Stream[_U]:
        ...

    @overload
    def __rfloordiv__(self, other: Any) -> Stream[_U] | It[_T, _U]:
        ...

    def __rfloordiv__(self, other: Any) -> Stream[_U] | It[_T, _U]:
        if isinstance(other, Stream):
            return other.amap(self.it_make_fn())
        self._it_ops.append(SurrogateOperation("__rfloordiv__", (other,)))
        return self

    @overload
    def __rmod__(self: It[_T, bool], other: Stream[_T]) -> Stream[_T]:
        ...

    @overload
    def __rmod__(self: It[_T, _U], other: Any) -> Stream[_T] | It[_T, _U]:
        ...

    def __rmod__(self, other: Any) -> Stream[_T] | It[_T, _U]:
        if isinstance(other, Stream):
            return other.afilter(self.it_make_fn())  # type: ignore
        self._it_ops.append(SurrogateOperation("__rmod__", (other,)))
        return self
