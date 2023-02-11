from __future__ import annotations

from datetime import timedelta
from typing import *

T = TypeVar("T")


class Animatable(Protocol):

    # Should be able to interpolate between two values
    def __add__(self: Animatable, other: Animatable) -> Animatable:
        ...

    def __sub__(self: Animatable, other: Animatable) -> Animatable:
        ...

    def __mul__(self: Animatable, other: Animatable) -> Animatable:
        ...

    def __truediv__(self: Animatable, other: Animatable) -> Animatable:
        ...


class NoValueT:
    __slots__ = ()

    instance: NoValueT

    def __new__(cls) -> NoValueT:
        if not hasattr(cls, "instance"):
            cls.instance = super().__new__(cls)
        return cls.instance


NoValue = NoValueT()

_AnimatableT = TypeVar("_AnimatableT", bound=Animatable)


class AnimatedProperty(Generic[_AnimatableT]):
    def __init__(
        self,
        initial_value: _AnimatableT,
    ):
        self._value = initial_value

    def __set_name__(self, owner: Type[object] | None, name: str) -> None:
        self.name = name

    def __set__(self, instance: object, value: _AnimatableT) -> None:
        self.value = value

    def __get__(
        self, instance: object | None, owner: Type[object]
    ) -> _AnimatableT | AnimatedProperty[_AnimatableT]:
        if instance is None:
            return self
        else:
            return self.value

    def __str__(self) -> str:
        return f"AnimatedProperty({self.name} :: {self._value})"


class Circle:

    x = AnimatedProperty(0)


print(Circle.x)
