from __future__ import annotations

import asyncio
import time
from datetime import datetime

import math

from asyncio import Event, Future
from typing import (
    Literal,
    cast,
    SupportsFloat,
    Generator,
    Generic,
    TypeVar,
    Callable,
    overload,
    Type,
)

_T = TypeVar("_T")


class Fuse:
    """Similar to asyncio.Event, but can only be set once."""

    def __init__(self) -> None:
        self._fut: Future[None] = asyncio.Future()

    def set(self) -> None:
        """Set the fuse."""
        self._fut.set_result(None)

    def is_set(self) -> bool:
        """Return True if the fuse is set."""
        return self._fut.done()

    async def wait(self) -> None:
        """Wait for the fuse to be set."""
        await self._fut


class SwitchEvent:
    def __init__(self, initial_state: bool = False) -> None:
        super().__init__()
        self._ev_on = Event()
        self._ev_off = Event()
        self._state: bool = initial_state
        self.set_state(initial_state)

    def set_state(self, state: bool) -> None:
        """Set the switch to a specific state.

        Args:
            state: The state to set.
        """
        if state:
            self.set()
        else:
            self.clear()

    def set(self) -> None:
        """Turn the switch on."""

        if not self._state:
            self._state = True
            self._ev_on.set()
            self._ev_off.clear()

    def clear(self) -> None:
        """Turn the switch off."""
        if self._state:
            self._state = False
            self._ev_off.set()
            self._ev_on.clear()

    def is_set(self) -> bool:
        """Return True if the switch is on."""
        return self._state

    def is_clear(self) -> bool:
        """Return True if the switch is off."""
        return not self._state

    async def wait(self) -> None:
        """Wait for the switch to be turned on."""
        if not self._state:
            await self._ev_on.wait()

    async def wait_clear(self) -> None:
        """Wait for the switch to be turned off."""
        if self._state:
            await self._ev_off.wait()

    async def wait_for(self, state: bool) -> None:
        """Wait for the switch to be turned on or off.

        Args:
            state: The state to wait for.
        """
        if state:
            await self.wait()
        else:
            await self.wait_clear()

    async def wait_toggle(self) -> None:
        """Wait for the switch to be change states."""
        if self._state:
            await self.wait_clear()
        else:
            await self.wait()

    async def wait_toggle_to(self, state: bool) -> None:
        """Wait for the switch to change from False to True, or True to False.

        Args:
            state: The state to wait for.
        """
        if state:
            await self.wait_clear()
            await self.wait()
        else:
            await self.wait()
            await self.wait_clear()


class EventfulCounter(SupportsFloat):
    """EventfulCounter

    This class represents a counter that can be incremented or decremented, and
    also has (optional) minimum and maximum values that it can reach.

    The counter has SwitchEvent instances for being at/beyond the minimum and
    maximum values. This allows waiting for the counter to reach the minimum value,
    to reach the maximum value, to cross from below the minimum value to above the
    minimum value, or to cross from above to below the maximum value.

    The counter can be incremented or decremented by any amount with the inc() and
    dec() methods, or by using the += and -= operators. The counter can also be
    set to a specific value with the set() method.

    If the clamp_to_bounds flag is set, the counter will be clamped to the minimum
    and maximum values when it reaches them. Otherwise, it will continue to
    increment or decrement past them.

    This class inherits from SupportsFloat, which means it can be cast to a
    float using the float() function. It also supports comparison using the
    >, <, and == operators. It can be cast to a boolean using the
    bool() function, which returns True if the counter has a value greater
    than 0.

    Args:
        initial_value: The initial value of the counter. Defaults to 0.

        max_value: The maximum value that the counter can reach, or None. If none, the
            counter has no maximum value. Defaults to None.

        min_value: The minimum value that the counter can reach, or None. If none, the
            counter has no minimum value. Defaults to None.

        clamp_to_bounds: Whether the counter should be clamped to the minimum and maximum values.
            Defaults to False.
    """

    def __init__(
        self,
        initial_value: float = 0,
        max_value: float | None = None,
        min_value: float | None = None,
        clamp_to_bounds: bool = False,
    ) -> None:
        self._counter: float = initial_value
        self._max_value = max_value
        self._min_value = min_value
        self._clamp_to_bounds = clamp_to_bounds
        self._min_ev = SwitchEvent()
        self._max_ev = SwitchEvent()
        self.set(initial_value)

    def inc(self, by: float = 1) -> None:
        """Increment the counter."""
        self.set(self._counter + by)

    def dec(self, by: float = 1) -> None:
        """Decrement the counter."""
        self.set(self._counter - by)

    @property
    def min_value(self) -> float | None:
        """Return the minimum value of the counter."""
        return self._min_value

    @min_value.setter
    def min_value(self, value: float | None) -> None:
        if value is None:
            self._min_value = None
            self._min_ev.set_state(False)
        else:
            if self._max_value is not None and value > self._max_value:
                raise ValueError("min_value cannot be greater than max_value")
            self._min_value = value
            self.set(self._counter)

    @property
    def max_value(self) -> float | None:
        """Return the maximum value of the counter."""
        return self._max_value

    @max_value.setter
    def max_value(self, value: float | None) -> None:
        if value is None:
            self._max_value = None
            self._max_ev.set_state(False)
        else:
            if self._min_value is not None and value < self._min_value:
                raise ValueError("max_value cannot be less than min_value")
            self._max_value = value
            self.set(self._counter)

    def set(self, value: float) -> None:
        """Set the counter to a specific value."""
        self._counter = value

        if self._max_value is not None and self._counter >= self._max_value:
            self._max_ev.set()
            if self._clamp_to_bounds:
                self._counter = self._max_value

        if self._min_ev.is_set() and self._counter > cast(float, self._min_value):
            self._min_ev.clear()

        if self._min_value is not None and self._counter <= self._min_value:
            self._min_ev.set()
            if self._clamp_to_bounds:
                self._counter = self._min_value

        if self._max_ev.is_set() and self._counter < cast(float, self._max_value):
            self._max_ev.clear()

    def __iadd__(self, other: float) -> EventfulCounter:
        self.inc(by=other)
        return self

    def __isub__(self, other: float) -> EventfulCounter:
        self.dec(by=other)
        return self

    def __float__(self) -> float:
        return float(self._counter)

    def __gt__(self, other: SupportsFloat) -> bool:
        return float(self) > float(other)

    def __lt__(self, other: SupportsFloat) -> bool:
        return float(self) < float(other)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SupportsFloat):
            return float(self) == float(other)
        return False

    def __bool__(self) -> bool:
        return bool(self._counter)

    def min_is_set(self) -> bool:
        """Return True if the counter is at or below the minimum value."""
        return self._min_ev.is_set()

    def max_is_set(self) -> bool:
        """Return True if the counter is at or above the maximum value."""
        return self._max_ev.is_set()

    async def wait_max(self) -> None:
        """Wait until the counter reaches its maximum value.

        Returns immediately if it already has.
        """
        await self._max_ev.wait()

    async def wait_min(self) -> None:
        """Wait until the counter reaches its minimum value.

        Returns immediately if it already has.
        """
        await self._min_ev.wait()

    async def wait_max_clear(self) -> None:
        """Wait for the counter to be below its maximum value.

        Returns immediately if it already is.
        """
        await self._max_ev.wait_clear()

    async def wait_min_clear(self) -> None:
        """Wait for the counter to be above its minimum value.

        Returns immediately if it already is.
        """
        await self._min_ev.wait_clear()

    def __str__(self) -> str:
        return (
            f"<{self.__class__.__name__}: {self._counter} "
            f"(min: {self._min_value}, max: {self._max_value})>"
        )

    __repr__ = __str__


class Pulse:
    """A pulse that can be triggered and waited for.

    Waiting for a pulse will block until the pulse is triggered, and will return
    the time at which the pulse was triggered. Alternatively, the pulse can be given
    a function to call when it is triggered. In this case, the return value of waiting
    on the pulse will be the result of calling the function.


    Examples:

        >>> async def demo_pulse() -> None:
        ...      # Creat an instance of Pulse, and trigger it a couple of times.
        ...      pulse = Pulse()
        ...
        ...      async def wait_pulse() -> None:
        ...          print("Waiting for pulse")
        ...          t = await pulse.wait()
        ...          print("Pulse fired!")
        ...          # print(f"Pulse received at {t}")
        ...          t = await pulse.wait()
        ...          print("Pulse fired!")
        ...          # print(f"Pulse received at {t}")
        ...
        ...      [asyncio.create_task(wait_pulse()) for _ in range(3)]
        ...      pulse.fire()
        ...      await asyncio.sleep(1)
        ...      pulse.fire()
        ...      await asyncio.sleep(1)
        ...      pulse.fire()
        >>> asyncio.run(demo_pulse())
        Waiting for pulse
        Waiting for pulse
        Waiting for pulse
        Pulse fired!
        Pulse fired!
        Pulse fired!
        Pulse fired!
        Pulse fired!
        Pulse fired!
    """

    __slots__ = ("_fut",)

    def __init__(self) -> None:
        # self._waiters: set[asyncio.Future[float]] = set()
        self._fut: Future[float] = asyncio.get_event_loop().create_future()

    def fire(self) -> None:
        """Fire the pulse, waking up all waiters."""
        self._fut.set_result(time.time())
        self._fut = asyncio.get_event_loop().create_future()

    async def wait(self) -> float:
        """Wait for the pulse to be fired.

        Returns a Future that will be resolved when the pulse is fired.
        The future's result will be the time at which the pulse was fired, as per time.time().
        """
        return await self._fut

    def __await__(self) -> Generator[None, None, float]:
        """Wait for the pulse to be fired.

        Returns the time at which the pulse was fired, as given by time.time().
        """
        return self._fut.__await__()


if __name__ == "__main__":

    async def demo_pulse() -> None:
        pulse = Pulse()

        async def wait_pulse() -> None:
            print("Waiting for pulse")
            t = await pulse.wait()
            print("Pulse fired! Time:", t)
            t = await pulse.wait()
            print("Pulse fired! Time:", t)

        [asyncio.create_task(wait_pulse()) for _ in range(3)]
        pulse.fire()
        await asyncio.sleep(1)
        pulse.fire()
        await asyncio.sleep(1)
        pulse.fire()

    asyncio.run(demo_pulse())


__all__ = ("EventfulCounter", "Fuse", "SwitchEvent", "Pulse")
