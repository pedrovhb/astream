from __future__ import annotations

import functools
from collections import deque
from typing import Any, Callable, ParamSpec, TypeVar, cast, overload

from astream import ensure_coro_fn
from astream.protocols.type_aliases import CoroT, R, T

A = TypeVar("A")
B = TypeVar("B")
C = TypeVar("C")
D = TypeVar("D")
E = TypeVar("E")
F = TypeVar("F")

P = ParamSpec("P")


def compose_two(
    fn1: Callable[P, T] | Callable[P, CoroT[T]],
    fn2: Callable[[T], R] | Callable[[T], CoroT[R]],
) -> Callable[P, CoroT[R]]:
    """Compose two sync or async functions, and return an async function.

    Args:
        fn1: The first function to compose.
        fn2: The second function to compose.

    Returns:
        The composed function.
    """
    fn1_async = cast(Callable[P, CoroT[T]], ensure_coro_fn(fn1))
    fn2_async = cast(Callable[[T], CoroT[R]], ensure_coro_fn(fn2))

    async def _composed(*args: P.args, **kwargs: P.kwargs) -> R:
        return await fn2_async(await fn1_async(*args, **kwargs))

    return _composed


@overload
def compose(
    __fn1: Callable[P, A] | Callable[P, CoroT[A]],
    __fn2: Callable[[A], B] | Callable[[A], CoroT[B]],
    __fn3: Callable[[B], C] | Callable[[B], CoroT[C]],
    __fn4: Callable[[C], D] | Callable[[C], CoroT[D]],
    __fn5: Callable[[D], E] | Callable[[D], CoroT[E]],
    __fn6: Callable[[E], F] | Callable[[E], CoroT[F]],
    __fn7: Callable[[F], Any] | Callable[[F], CoroT[Any]],
    *funcs: Callable[[Any], Any] | Callable[[Any], CoroT[Any]],
) -> Callable[P, CoroT[Any]]:
    ...


@overload
def compose(
    __fn1: Callable[P, A] | Callable[P, CoroT[A]],
    __fn2: Callable[[A], B] | Callable[[A], CoroT[B]],
    __fn3: Callable[[B], C] | Callable[[B], CoroT[C]],
    __fn4: Callable[[C], D] | Callable[[C], CoroT[D]],
    __fn5: Callable[[D], E] | Callable[[D], CoroT[E]],
    __fn6: Callable[[E], F] | Callable[[E], CoroT[F]],
    *funcs: Any,
) -> Callable[P, CoroT[F]]:
    ...


@overload
def compose(
    __fn1: Callable[P, A] | Callable[P, CoroT[A]],
    __fn2: Callable[[A], B] | Callable[[A], CoroT[B]],
    __fn3: Callable[[B], C] | Callable[[B], CoroT[C]],
    __fn4: Callable[[C], D] | Callable[[C], CoroT[D]],
    __fn5: Callable[[D], E] | Callable[[D], CoroT[E]],
    *funcs: Any,
) -> Callable[P, CoroT[E]]:
    ...


@overload
def compose(
    __fn1: Callable[P, A] | Callable[P, CoroT[A]],
    __fn2: Callable[[A], B] | Callable[[A], CoroT[B]],
    __fn3: Callable[[B], C] | Callable[[B], CoroT[C]],
    __fn4: Callable[[C], D] | Callable[[C], CoroT[D]],
    *funcs: Any,
) -> Callable[P, CoroT[D]]:
    ...


@overload
def compose(
    __fn1: Callable[P, A] | Callable[P, CoroT[A]],
    __fn2: Callable[[A], B] | Callable[[A], CoroT[B]],
    __fn3: Callable[[B], C] | Callable[[B], CoroT[C]],
    *funcs: Any,
) -> Callable[P, CoroT[C]]:
    ...


@overload
def compose(
    __fn1: Callable[P, A] | Callable[P, CoroT[A]],
    __fn2: Callable[[A], B] | Callable[[A], CoroT[B]],
    *funcs: Any,
) -> Callable[P, CoroT[B]]:
    ...


@overload
def compose(
    __fn1: Callable[P, A] | Callable[P, CoroT[A]],
    *funcs: Any,
) -> Callable[P, CoroT[A]]:
    ...


def compose(
    *funcs: Any,
) -> Callable[..., CoroT[Any]]:
    """Compose multiple sync or async functions, and return an async function


    Returns:
        The composed function.
    """
    fns = deque(ensure_coro_fn(fn) for fn in funcs)
    if len(fns) == 1:
        return fns[0]
    return functools.reduce(compose_two, funcs)  # type: ignore
