from __future__ import annotations

import asyncio
import functools
import operator
from
from abc import ABCMeta
from collections import defaultdict
from copy import deepcopy, copy
from functools import partial, partialmethod
from types import SimpleNamespace
from typing import (
    Generic,
    Callable,
    Iterable,
    TypeVar,
    ParamSpec,
    cast,
    Any,
    Mapping,
    NamedTuple,
    Dict, Tuple,
)

import rich
from rich import inspect
from rich.table import Table

from astream import Stream, arange, stream
from astream.stream import StreamMapper, StreamFilterer, StreamFlatMapper

_T = TypeVar("_T")
_U = TypeVar("_U")
_R = TypeVar("_R")
_P = ParamSpec("_P")

_A = TypeVar("_A")
_B = TypeVar("_B")
_C = TypeVar("_C")


def _op_call(obj: Any, /, *args: Any, **kwargs: Any) -> Any:
    """Same as obj(*args, **kwargs)."""
    return obj(*args, **kwargs)


class F(Generic[_T, _U]):
    partial: Callable[[_T], _U]

    def __init__(self, fn: Any) -> None:
        if isinstance(fn, LazyProxyObj):

            # todo - properly implement cases in which an argument to a lazy proxy is a lazy proxy.
            #  I'm sure it's possible, but it's not a priority right now. The below is a broken
            #  implementation of this. For some fun, try out the below with something like
            #  print(F(it - 1 + it * 5)(1))
            ops = deepcopy(fn.lazy_proxy_operations)
            fn.lazy_proxy_operations.clear()
            def _fn(obj: _T) -> _U:
                for op in ops:
                    # args = list(op.args)
                    # for i, arg in enumerate(args):
                    #     if isinstance(arg, LazyProxyObj):
                    #         print("LazyProxyObj in args")
                    #         args[i] = obj
                    # print(f"{op.func}({obj}, {op.args})")
                    obj = op(obj)
                return cast(_U, obj)

            # For now, raise an error if a lazy proxy is passed as an argument to another lazy proxy.

            # if any(op for op in ops if any(isinstance(arg, LazyProxyObj) for arg in op.args)):
            #     raise ValueError("LazyProxyObj in args is not (yet) supported.")
            #
            # # Order ops by precedence
            ordered_ops = defaultdict(list)
            precedence = {
                operator.add: -1,
                operator.iadd: -1,
                operator.sub: -1,
                operator.isub: -1,
                operator.mul: -2,
                operator.imul: -2,
                operator.truediv: -2,
                operator.itruediv: -2,
                operator.floordiv: -2,
                operator.ifloordiv: -2,
                operator.mod: -2,
                operator.pow: -3,
                operator.lshift: -4,
                operator.rshift: -4,
                operator.and_: -5,
                operator.xor: -6,
                operator.or_: -7,
                operator.neg: -8,
                operator.pos: -8,
                operator.abs: -8,
                operator.invert: -8,
                operator.lt: -9,
                operator.le: -9,
                operator.eq: -9,
                operator.ne: -9,
                operator.gt: -9,
                operator.ge: -9,
                operator.contains: -9,
                operator.not_: -9,
                operator.is_: -9,
                operator.is_not: -9,
                operator.index: -9,
                operator.getitem: -9,
                operator.setitem: -9,
                operator.delitem: -9,
                operator.attrgetter: -9,
                operator.itemgetter: -9,
                operator.methodcaller: -9,
                _op_call: -9,
            }
            for op in ops:
                try:
                    print(op.func, precedence[op.func])
                    ordered_ops[precedence[op.func]].append(op)
                except:
                    print(op, "not in precedence")

            for i in ordered_ops:
                print(i, ordered_ops[i], [oop.args for oop in ordered_ops[i]])

            # for op in ops:
            #     ordered_ops[precedence[op.func]].append(op)
            #
            #
            # def _fn(obj: _T) -> _U:
            #     for op in ops:
            #         print(f"op: {op}, obj: {obj}")
            #         obj = op(obj)
            #     return cast(_U, obj)

            # fn.lazy_proxy_operations.clear()

            self.partial = _fn
        else:
            self.partial = fn


    def __call__(self, obj: _T) -> _U:
        # todo - split unitialized/initialized F objects into two classes
        return self.partial(obj)

    def __stream_map__(self, stream: Stream[_T]) -> Stream[_U]:
        return stream.amap(self.partial)

    def __stream_filter__(self: F[_T, bool], stream: Stream[_T]) -> Stream[_T]:
        return stream.afilter(self.partial)

    def __stream_flatmap__(self: F[Iterable[_A], _U], stream: Stream[Iterable[_A]]) -> Stream[_U]:
        return stream.aflatmap(self.partial)

    # def __str__(self) -> str:
    #     return f"F({self.partial})"
    #
    # def __repr__(self) -> str:
    #     return f"F({self.partial})"


# Proxy object - after operations are performed on it, it will be replaced with a function which
# takes an object and performs the same operations on it.

def partial_flipped(func: Callable[..., _R], /, *args: Any, **kwargs: Any) -> Callable[..., _R]:
    """Same as partial(func, *args, **kwargs), but with the arguments flipped."""
    @functools.wraps(func)
    def _fn(*args2: Any, **kwargs2: Any) -> _R:
        return func(*args2, *args, **kwargs, **kwargs2)
    _fn.args = args
    _fn.func = func
    return _fn


class LazyProxyObj(Mapping[Any, Any]):
    __slots__ = ("lazy_proxy_operations",)

    def __init__(self) -> None:
        self.lazy_proxy_operations: defaultdict[int, list[Any]] = defaultdict(list)



    # def __call__(self, *args: Any, **kwargs: Any) -> LazyProxyObj:
    #     p = partial(_op_call, *args, **kwargs)
    #     self.lazy_proxy_operations.append(p)
    #     return self

    def __len__(self) -> int:
        self.lazy_proxy_operations.append(len)
        return 1

    def __bool__(self) -> bool:
        self.lazy_proxy_operations.append(bool)
        return False

    def __int__(self) -> int:
        self.lazy_proxy_operations.append(int)
        return 1

    def __float__(self) -> float:
        self.lazy_proxy_operations.append(float)
        return 1.0

    def __str__(self) -> str:
        self.lazy_proxy_operations.append(str)
        return f"LazyProxyObj({self.lazy_proxy_operations})"

    def __repr__(self) -> str:
        self.lazy_proxy_operations.append(repr)
        return f"LazyProxyObj({self.lazy_proxy_operations})"

    def __deepcopy__(self, memo: dict[int, Any]) -> LazyProxyObj:
        return self

    def _do_binary_op(self, op: Callable[[Any, Any], Any], other: Any, flipped: bool=True) -> LazyProxyObj:
        print(f"op: {op}, other: {other}, flipped: {flipped}")
        p = partial_flipped(op, other) if flipped else partial(op, other)
        self.lazy_proxy_operations.append(p)
        return self

    def _do_unary_op(self, op: Callable[[Any], Any]) -> LazyProxyObj:
        self.lazy_proxy_operations.append(op)
        return self

    __add__ = partialmethod(_do_binary_op, operator.add)
    __sub__ = partialmethod(_do_binary_op, operator.sub)
    __mul__ = partialmethod(_do_binary_op, operator.mul)
    __truediv__ = partialmethod(_do_binary_op, operator.truediv)
    __floordiv__ = partialmethod(_do_binary_op, operator.floordiv)
    __mod__ = partialmethod(_do_binary_op, operator.mod)
    __pow__ = partialmethod(_do_binary_op, operator.pow)
    __lshift__ = partialmethod(_do_binary_op, operator.lshift)
    __rshift__ = partialmethod(_do_binary_op, operator.rshift)
    __and__ = partialmethod(_do_binary_op, operator.and_)
    __xor__ = partialmethod(_do_binary_op, operator.xor)
    __or__ = partialmethod(_do_binary_op, operator.or_)
    __radd__ = partialmethod(_do_binary_op, operator.add, flipped=False)
    __rsub__ = partialmethod(_do_binary_op, operator.sub, flipped=False)
    __rmul__ = partialmethod(_do_binary_op, operator.mul, flipped=False)
    __rtruediv__ = partialmethod(_do_binary_op, operator.truediv, flipped=False)
    __rfloordiv__ = partialmethod(_do_binary_op, operator.floordiv, flipped=False)
    __rmod__ = partialmethod(_do_binary_op, operator.mod, flipped=False)
    __rpow__ = partialmethod(_do_binary_op, operator.pow, flipped=False)
    __rlshift__ = partialmethod(_do_binary_op, operator.lshift, flipped=False)
    __rrshift__ = partialmethod(_do_binary_op, operator.rshift, flipped=False)
    __rand__ = partialmethod(_do_binary_op, operator.and_, flipped=False)
    __rxor__ = partialmethod(_do_binary_op, operator.xor, flipped=False)
    __ror__ = partialmethod(_do_binary_op, operator.or_, flipped=False)
    __iadd__ = partialmethod(_do_binary_op, operator.iadd)
    __isub__ = partialmethod(_do_binary_op, operator.isub)
    __imul__ = partialmethod(_do_binary_op, operator.imul)
    __itruediv__ = partialmethod(_do_binary_op, operator.itruediv)
    __ifloordiv__ = partialmethod(_do_binary_op, operator.ifloordiv)
    __imod__ = partialmethod(_do_binary_op, operator.imod)
    __ipow__ = partialmethod(_do_binary_op, operator.ipow)
    __ilshift__ = partialmethod(_do_binary_op, operator.ilshift)
    __irshift__ = partialmethod(_do_binary_op, operator.irshift)
    __iand__ = partialmethod(_do_binary_op, operator.iand)
    __ixor__ = partialmethod(_do_binary_op, operator.ixor)
    __ior__ = partialmethod(_do_binary_op, operator.ior)

    __iter__ = partialmethod(_do_unary_op, iter)  # type: ignore
    __next__ = partialmethod(_do_unary_op, next)
    __neg__ = partialmethod(_do_unary_op, operator.neg)
    __pos__ = partialmethod(_do_unary_op, operator.pos)
    __abs__ = partialmethod(_do_unary_op, operator.abs)
    __invert__ = partialmethod(_do_unary_op, operator.invert)

    def __getattr__(self, item: str) -> LazyProxyObj:
        p = partial_flipped(getattr, item)
        self.lazy_proxy_operations.append(p)
        return self

    __gt__ = partialmethod(_do_binary_op, operator.gt)
    __ge__ = partialmethod(_do_binary_op, operator.ge)
    __lt__ = partialmethod(_do_binary_op, operator.lt)
    __le__ = partialmethod(_do_binary_op, operator.le)

    def __getitem__(self, item: Any) -> LazyProxyObj:
        p = partial_flipped(operator.getitem, item)
        self.lazy_proxy_operations.append(p)
        return self

    def __eq__(self, other: Any) -> bool:
        p = partial_flipped(operator.eq, other)
        self.lazy_proxy_operations.append(p)
        return self  # type: ignore

    def __ne__(self, other: Any) -> bool:
        p = partial_flipped(operator.ne, other)
        self.lazy_proxy_operations.append(p)
        return self  # type: ignore

    def __setitem__(self, key: Any, value: Any) -> None:
        p = partial_flipped(operator.setitem, key, value)
        self.lazy_proxy_operations.append(p)

    def __delitem__(self, key: Any) -> None:
        p = partial_flipped(operator.delitem, key)
        self.lazy_proxy_operations.append(p)

    def __call__(self, *args: Any, **kwargs: Any) -> LazyProxyObj:
        p = partial_flipped(_op_call, *args, **kwargs)
        self.lazy_proxy_operations.append(p)
        return self

    # todo -
    #  def keys(self):
    #  def values(self):
    #  def items(self):
    #  (for unpacking)

class Op(NamedTuple):
    op: Callable
    args: Tuple[Any, ...] = ()
    kwargs: Dict[str, Any] = {}

    def resolve(self, obj: Any) -> Any:
        return self.op(obj, *self.args, **self.kwargs)

class LazyProxyObjPrecedence:
    __slots__ = ("lazy_proxy_operations",)
    """
    
    Precedence order - 
    
    x[index], x[index:index], x(arguments...), x.attribute
        Subscription, slicing, call, attribute reference
        
    await x
        Await expression
        
    **
        Exponentiation
    
    +x, -x, ~x
        Positive, negative, bitwise NOT
        
    *, @, /, //, %
        Multiplication, matrix multiplication, division, floor division, modulo
        
    *, @, /, //, %
        Multiplication, matrix multiplication, division, floor division, modulo
        
    +, -
        Addition and subtraction
        
    <<, >>
        Bitwise shift operators
        
    &
        Bitwise AND
        
    ^
        Bitwise XOR
        
    |
        Bitwise OR
        
    in, not in, is, is not, <, <=, >, >=, !=, ==
        Comparisons, including membership tests and identity tests
        
    not x
        Boolean NOT
        
    and
        Boolean AND
        
    or
        Boolean OR
    """

    def __init__(self) -> None:
        self.lazy_proxy_operations: defaultdict[int, list[Any]] = defaultdict(list)

    # def __getitem__(self, item: Any) -> LazyProxyObjPrecedence:
    #     # getitem precedence is 1
    #     self.lazy_proxy_operations[1].append(lambda x: x[item])
    #     return self

    @staticmethod
    def _add_precedence(precedence: int, func: Callable, flip_args: bool=False) -> Callable[..., LazyProxyObjPrecedence]:
        def _f(*args, **kwargs) -> Any:
            self = args[0]
            if flip_args:
                args = args[::-1]
            self.lazy_proxy_operations[precedence].append((func, args, kwargs))
            return self
        return _f
    # _add_precedence = meth(_add_precedence)

    __getitem__ = _add_precedence(1, lambda x, y: x[y])
    __getattr__ = _add_precedence(1, lambda x, y: getattr(x, y))
    __call__ = _add_precedence(1, lambda x, *args, **kwargs: x(*args, **kwargs))

    __pow__ = _add_precedence(3, lambda x, y: x ** y)
    __rpow__ = _add_precedence(3, lambda x, y: y ** x)

    __add__ = _add_precedence(5, lambda x, y: x + y)
    __radd__ = _add_precedence(5, lambda x, y: y + x)

    def __str__(self) -> str:
        return f"<LazyProxyObjPrecedence {self.lazy_proxy_operations}>"

    def __repr__(self) -> str:
        return f"<LPO>"


lp = LazyProxyObjPrecedence()

x = lp[4].something

print(x)
