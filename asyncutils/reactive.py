from __future__ import annotations

import asyncio
import inspect
from functools import cached_property, partialmethod
from types import NotImplementedType
from typing import (
    Generic,
    TypeVar,
    cast,
    AsyncIterable,
    overload,
    TypeAlias,
    Protocol,
    NamedTuple,
    Any,
)
from collections.abc import (
    Awaitable,
    Callable,
    Coroutine,
    Iterable,
    Iterator,
    Mapping,
    MutableMapping,
    MutableSequence,
    Sequence,
    Set,
)

import wrapt

from asyncutils import atee
from asyncutils.afilter import _SentinelType, afilter
from asyncutils.atee import ClonableAsyncIterableWrapper

T = TypeVar("T")
_OwnerT = TypeVar("_OwnerT")

_NoDefaultReactiveValue = _SentinelType()
NoPreviousValue = _SentinelType()


# todo - use the generic tuple when it's available, in the so so distant future
#  https://github.com/python/mypy/issues/685
# class UpdateT(Generic[_OwnerT, T], NamedTuple):
class ReactiveUpdate(Generic[T, _OwnerT]):

    __slots__ = ("instance", "old_value", "new_value")

    def __init__(self, instance: _OwnerT, old_value: T, new_value: T) -> None:
        self.instance = instance
        self.old_value = old_value
        self.new_value = new_value

    def __str__(self) -> str:
        return (
            f"<UpdateT("
            f"instance={self.instance!r}, "
            f"old_value={self.old_value!r}, "
            f"new_value={self.new_value!r}"
            f")>"
        )

    def __repr__(self) -> str:
        return str(self)

    def __rich_repr__(self) -> str:
        return (
            f"[green]UpdateT[/green]("
            f"[blue]instance[/]=[bold]{self.instance!r}[/], "
            f"[dark blue]old_value[/]={self.old_value!r}[/], "
            f"[light blue]new_value[/]={self.new_value!r}[/]"
            f")"
        )

    def __iter__(self) -> tuple[_OwnerT, T, T]:
        return self.instance, self.old_value, self.new_value


class ReactiveWrapper(wrapt.ObjectProxy, Generic[T, _OwnerT]):
    def __init__(
        self,
        wrapped: T | _SentinelType,
        change_aiter_fn: Callable[[], AsyncIterable[ReactiveUpdate[T, _OwnerT]]],
    ) -> None:

        super().__init__(wrapped)
        self._self_change_aiter_fn = change_aiter_fn

    def changes(self) -> AsyncIterable[ReactiveUpdate[T, _OwnerT]]:
        return self._self_change_aiter_fn()

    def _inplace_dunder_method(
        self,
        other: T | ReactiveWrapper[T, _OwnerT],
        method_name: str,
    ) -> ReactiveWrapper[T, _OwnerT] | NotImplementedType:
        if isinstance(other, ReactiveWrapper):
            other = other.__wrapped__

        if hasattr(self.__wrapped__, method_name):
            # todo - does it make sense to ever use the inplace methods?
            method = getattr(self.__wrapped__, method_name)
            result = method(other)
        elif hasattr(self.__wrapped__, (non_inplace_method := method_name.replace("__i", "__"))):
            method = getattr(self.__wrapped__, non_inplace_method)
            result = method(other)
        else:
            return NotImplemented

        return ReactiveWrapper(result, self._self_change_aiter_fn)

    __iadd__ = partialmethod(_inplace_dunder_method, method_name="__iadd__")
    __isub__ = partialmethod(_inplace_dunder_method, method_name="__isub__")
    __imul__ = partialmethod(_inplace_dunder_method, method_name="__imul__")
    __imatmul__ = partialmethod(_inplace_dunder_method, method_name="__imatmul__")
    __ifloordiv__ = partialmethod(_inplace_dunder_method, method_name="__ifloordiv__")
    __itruediv__ = partialmethod(_inplace_dunder_method, method_name="__itruediv__")
    __imod__ = partialmethod(_inplace_dunder_method, method_name="__imod__")
    __ipow__ = partialmethod(_inplace_dunder_method, method_name="__ipow__")
    __ilshift__ = partialmethod(_inplace_dunder_method, method_name="__ilshift__")
    __irshift__ = partialmethod(_inplace_dunder_method, method_name="__irshift__")
    __iand__ = partialmethod(_inplace_dunder_method, method_name="__iand__")
    __ixor__ = partialmethod(_inplace_dunder_method, method_name="__ixor__")
    __ior__ = partialmethod(_inplace_dunder_method, method_name="__ior__")

    # def __iadd__(self, other: T | ReactiveWrapper[T, _OwnerT]) -> ReactiveWrapper[T, _OwnerT]:
    #     if isinstance(other, ReactiveWrapper):
    #         other = other.__wrapped__
    #     return ReactiveWrapper(self.__wrapped__.__iadd__(other), self._self_change_aiter_fn)

    def __format__(self, format_spec: str) -> str:
        return cast(str, self.__wrapped__.__format__(format_spec))


# Create a descriptor that will be used to create a reactive property
class ReactiveProperty(Generic[T, _OwnerT]):
    def __init__(
        self,
        default_value: T | _SentinelType = _NoDefaultReactiveValue,
        default_factory: Callable[[], T] | None = None,
    ) -> None:
        self.default_value = default_value
        self.default_factory = default_factory

        self._change_queue = asyncio.Queue[ReactiveUpdate[T, _OwnerT]]()

    @property
    def private_name(self) -> str:
        return f"_reactive_{self.name}"

    def create_reactive_wrapper(
        self, value: T | _SentinelType, instance: _OwnerT | type[_OwnerT]
    ) -> ReactiveWrapper[T, _OwnerT]:
        async def _instance_aiter() -> AsyncIterable[ReactiveUpdate[T, _OwnerT]]:

            async for change in self.changes():
                if change.instance is instance:
                    yield change

        return ReactiveWrapper(value, change_aiter_fn=_instance_aiter)

    def __set_name__(self, owner_cls: type[_OwnerT], name: str) -> None:

        # Set the name of the property
        self.name = name
        self._owner_cls = owner_cls

        # Create a private attribute to store the value
        if self.default_factory is not None:
            setattr(
                owner_cls,
                self.private_name,
                self.create_reactive_wrapper(self.default_factory(), owner_cls),
            )
        elif not isinstance(self.default_value, _SentinelType):
            setattr(
                owner_cls,
                self.private_name,
                self.create_reactive_wrapper(self.default_value, owner_cls),
            )
        else:
            # todo - what when no default value on class?
            setattr(
                owner_cls,
                self.private_name,
                self.create_reactive_wrapper(NoPreviousValue, owner_cls),
            )

    def __set__(self, instance: _OwnerT, value: T) -> None:

        if instance is None:
            # Instance creation i.e. __new__?
            # Check if __new__ is on the stack:
            is_new = bool(inspect.stack()[1].function == "__new__")
            if not is_new:
                raise ValueError("Cannot set a property on a class")
            return
        if isinstance(value, _SentinelType):
            raise ValueError("Cannot set a property to a sentinel value")

        wrapped = getattr(instance, self.private_name)
        # print(f"value: {value!r}, wrapped: {wrapped!r}, old: {wrapped.__wrapped__!r}")
        old_value = wrapped.__wrapped__

        new_value = value.__wrapped__ if isinstance(value, ReactiveWrapper) else value
        if isinstance(new_value, ReactiveWrapper):
            raise ValueError("nested reactive wrappers are not supported")
        # todo - maybe different for primitive/value types vs. reference types?
        new_wrapped = self.create_reactive_wrapper(new_value, instance)

        # todo - check if this value lingers around too long, delete manually if so
        setattr(instance, self.private_name, new_wrapped)

        if not isinstance(old_value, _SentinelType):
            self._change_queue.put_nowait(ReactiveUpdate(instance, old_value, value))

    @overload
    def __get__(self, instance: None, owner_cls: type[_OwnerT]) -> ReactiveProperty[T, _OwnerT]:
        ...

    @overload
    def __get__(self, instance: _OwnerT, owner_cls: type[_OwnerT]) -> ReactiveWrapper[T, _OwnerT]:
        ...

    def __get__(
        self,
        instance: _OwnerT | None,
        owner_cls: type[_OwnerT],
    ) -> ReactiveWrapper[T, _OwnerT] | ReactiveProperty[T, _OwnerT]:
        if instance is None:
            return self
        return cast(ReactiveWrapper[T, _OwnerT], getattr(instance, self.private_name))

    def __delete__(self, instance: _OwnerT) -> None:
        delattr(instance, self.private_name)

    def changes(self) -> AsyncIterable[ReactiveUpdate[T, _OwnerT]]:
        return self._changes.aclone()

    @cached_property
    def _changes(self) -> ClonableAsyncIterableWrapper[ReactiveUpdate[T, _OwnerT]]:
        """Return an async iterable of changes to this property"""

        async def _changes() -> AsyncIterable[ReactiveUpdate[T, _OwnerT]]:
            while True:

                change = await self._change_queue.get()
                if change.old_value is NoPreviousValue:
                    continue
                yield change

        return ClonableAsyncIterableWrapper(_changes())

    def __repr__(self) -> str:
        return f"<ReactiveProperty {self.name}>"

    def __str__(self) -> str:
        return f"ReactiveProperty {self.name}"


if __name__ == "__main__":

    class Foo:
        a = ReactiveProperty[int, "Foo"](2)
        b = ReactiveProperty[int, "Foo"](default_factory=lambda: 5)

        def __init__(self, a: int | None = None, b: int | None = None) -> None:
            if a is not None:
                self.a = a
            if b is not None:
                self.b = b

    t = Foo()
    u = Foo(1337)

    async def main() -> None:
        async def watch_changes() -> None:
            # async for i in t.a.changes():
            #     print(i)

            async for ch in t.a.changes():
                print(repr(ch))

        asyncio.create_task(watch_changes())
        asyncio.create_task(watch_changes())
        asyncio.create_task(watch_changes())

        for i in range(10):
            t.a = i
            await asyncio.sleep(0.1)

    asyncio.run(main())
