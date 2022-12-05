from __future__ import annotations

import asyncio

from asyncio import Event, Future
from typing import Literal

import math


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


class SwitchEvent(Event):
    """A switch that can be turned on and off.

    This is similar to asyncio.Event, but can be turned on and off, and can be
    reset to its initial state.
    """

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

    async def wait(self) -> Literal[True]:
        """Wait for the switch to be turned on."""
        return await self._ev_on.wait()

    async def wait_clear(self) -> Literal[True]:
        """Wait for the switch to be turned off."""
        return await self._ev_off.wait()

    async def wait_for(self, state: bool) -> Literal[True]:
        """Wait for the switch to be turned on or off.

        Args:
            state: The state to wait for.
        """
        if state:
            return await self.wait()
        else:
            return await self.wait_clear()

    async def wait_toggle(self) -> Literal[True]:
        """Wait for the switch to be toggled."""
        if self._state:
            return await self.wait_clear()
        else:
            return await self.wait()

    async def wait_toggle_to(self, state: bool) -> Literal[True]:
        """Wait for the switch to be toggled to a specific state.

        Args:
            state: The state to wait for.
        """
        if state:
            await self.wait_clear()
            return await self.wait()
        else:
            await self.wait()
            return await self.wait_clear()


class EventGeneratingCounter:
    """A counter that can be incremented and decremented, and has events that fire when it reaches
    its maximum or minimum value.

    """
    def __init__(
        self,
        initial_value: int = 0,
        max_value: int | None = None,
        min_value: int | None = None,
    ) -> None:

        self._counter: int = initial_value
        self._max_value = max_value if max_value is not None else math.inf
        self._min_value = min_value if min_value is not None else -math.inf
        self._min_ev = SwitchEvent()
        self._max_ev = SwitchEvent()

    def __iadd__(self, other: int) -> EventGeneratingCounter:
        self._counter += other
        if self._counter >= self._max_value:
            self._max_ev.set()
        if self._min_ev.is_set() and self._counter >= self._min_value:
            self._min_ev.clear()
        return self

    def __isub__(self, other: int) -> EventGeneratingCounter:
        self._counter -= other
        if self._counter <= self._min_value:
            self._min_ev.set()
        if self._max_ev.is_set() and self._counter <= self._max_value:
            self._max_ev.clear()
        return self

    def __int__(self) -> int:
        return self._counter

    def __bool__(self) -> bool:
        return bool(self._counter)

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

    inc = __iadd__
    dec = __isub__
