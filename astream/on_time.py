import asyncio
import itertools
import statistics
import time
from asyncio import Task
from datetime import datetime, timedelta
from typing import *

import math

from astream.event_like import Pulse
from astream.utils import ensure_coroutine_function

_P = ParamSpec("_P")


class OnTime:
    def __init__(
        self,
        period: float | timedelta,
        count: int | None = None,
        start: bool = True,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        self._iteration_count = 0
        self._ready_pulse = Pulse()
        self._done_event = asyncio.Event()

        if isinstance(period, timedelta):
            # Convert timedelta to a float representing the number of seconds
            period = period.total_seconds()
        self._period = period
        self._event_loop = loop or asyncio.get_event_loop()

        self._start_time = self._event_loop.time()
        self._min_drift: float | None = None
        self._max_drift: float | None = None
        self._mean_drift: float | None = None
        self._count = count
        self._started = False

        self._tick_handle: asyncio.TimerHandle | None = None
        self._periodic_tasks = set[Task[None]]()

        if start:
            self.start()

    def start(self) -> None:
        if self._started:
            raise RuntimeError("OnTime instance has already been started")
        self._started = True
        self._start_time = self._event_loop.time()
        self._tick()

    def _tick(self) -> None:
        """Fires off the periodic logic, and schedules the next iteration of itself.

        This method is called for the first time after `start`, and then schedules itself to be
        called again after the period has elapsed on each iteration.
        """
        if self.is_done:
            return

        # Stop scheduling iterations if the count has been reached
        if self._count is not None and self._iteration_count > self._count:
            self.stop()
            return

        self._ready_pulse.fire()
        next_time = self._iteration_count * self._period + self._start_time
        self._iteration_count += 1
        self._tick_handle = self._event_loop.call_at(next_time, self._tick)

    def stop(self) -> None:
        """Stops the OnTime instance from running."""
        if self._tick_handle is not None:
            self._tick_handle.cancel()
            self._done_event.set()
            for task in self._periodic_tasks:
                task.cancel()

    async def wait_done(self) -> None:
        """Waits until the OnTime instance has finished running (if it has a count) or has been
        stopped.
        """
        await self._done_event.wait()

    @property
    def period(self) -> timedelta:
        return timedelta(seconds=self._period)

    @property
    def iteration_count(self) -> int:
        return self._iteration_count

    @property
    def until_next(self) -> timedelta:
        """Returns the remaining time until the next iteration.

        Returns:
            The remaining time until the next iteration.

        Raises:
            RuntimeError: If the OnTime instance has not been started, or has been stopped.
        """
        if self._tick_handle is None:
            raise RuntimeError("OnTime instance has not been started")
        if self.is_done:
            raise RuntimeError("OnTime instance has finished running")
        return timedelta(seconds=self._tick_handle.when() - self._event_loop.time())

    @property
    def is_done(self) -> bool:
        return self._done_event.is_set()

    def __await__(self) -> Generator[None, None, float]:
        """Waits until the next iteration of the OnTime instance."""
        return self._ready_pulse.__await__()

    def run_periodically(
        self,
        fn: Callable[_P, object],
        to_thread: bool = False,
        *args: _P.args,
        **kwargs: _P.kwargs,
    ) -> Task[None]:
        """Runs a function or coroutine function at a fixed interval as per the OnTime instance.

        The function is called with the given arguments and keyword arguments, and is run in a
        separate task. The task is returned, and can be cancelled if necessary. It is marked
        as done when the OnTime instance is stopped, either by reaching the count or by calling
        `stop`.

        Args:
            fn: The function or coroutine function to run.
            to_thread: Whether to run the function in a thread. If True, the function must be a
                regular function, not a coroutine function.
            *args: The arguments to pass to the function.
            **kwargs: The keyword arguments to pass to the function.

        Returns:
            A Task that runs the function or coroutine function at a fixed interval.
        """

        if to_thread and asyncio.iscoroutinefunction(fn):
            raise ValueError("Cannot run a coroutine function in a thread")

        async def _run_periodically() -> None:
            _async_fn = ensure_coroutine_function(fn, to_thread)
            while True:
                await self
                await _async_fn(*args, **kwargs)

        task = asyncio.create_task(_run_periodically())
        self._periodic_tasks.add(task)
        return task


class DriftStats:
    def __init__(self) -> None:
        self.min: float | None = None
        self.max: float | None = None
        self.count = 0.0
        self.last: float | None = None
        self.mean = 0.0
        self.variance = 0.0

    def update(self, drift: float) -> None:
        self.count += 1
        self.last = drift
        if self.min is None or drift < self.min:
            self.min = drift
        if self.max is None or drift > self.max:
            self.max = drift

        if self.count == 1:
            self.mean = drift
        else:
            self.mean = (self.mean * (self.count - 1) + drift) / self.count
            self.variance = ((self.count - 2) * self.variance + (drift - self.mean) ** 2) / (
                self.count - 1
            )

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    def __repr__(self) -> str:
        return (
            f"DriftStats(\n"
            f"\tmin={self.min:.2f}, max={self.max:.2f}, mean={self.mean:.2f},\n"
            f"\tstddev={self.stddev:.2f}, count={self.count}, last={self.last:.2f}\n"
            f"\n)"
        )


if __name__ == "__main__":

    async def main() -> None:
        """Testing code (drift, with cpu, memory usage, etc over scalene)."""
        drifts: dict[float, DriftStats] = {
            t: DriftStats() for t in (0.001, 0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 1, 3, 10, 30, 100)
        }

        async def track_drift(period: float) -> None:
            """Tracks the drift of the OnTime instance."""
            initial_time = time.time()
            expected_time = (initial_time + period * i for i in itertools.count())
            on_time = OnTime(period, start=True)

            while True:
                t = await on_time
                drifts[period].update(t - next(expected_time))

        async def print_drifts() -> None:
            while True:
                await asyncio.sleep(1)
                s = []
                for p, ds in drifts.items():
                    s.append(f"{p}: {ds}")
                ss = "\n".join(s).replace("\n", "\033[")
                print(f"\033[2J\033[1;1H{'-' * 80}\n{ss}", flush=True)

        for p in drifts:
            asyncio.create_task(track_drift(p))
        asyncio.create_task(print_drifts())

        while True:
            await asyncio.sleep(100)

    asyncio.run(main())

__all__ = ("OnTime",)
