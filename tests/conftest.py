from __future__ import annotations

import time
from asyncio import TimerHandle
from contextvars import Context
from typing import Callable

import math
from _pytest.main import Session

import asyncio


class FastEventLoop(asyncio.SelectorEventLoop):
    """Fast loop go brr"""

    # Arbitrary large value
    TIME_DIVIDER = 1000000

    def call_at(
        self,
        when: float,
        callback: Callable[..., object],
        *args: object,
        context: Context | None = None,
    ) -> TimerHandle:
        when /= self.TIME_DIVIDER  # not totally sure why this works actually
        return super().call_at(when, callback, *args, context=context)


class EventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    _loop_factory = FastEventLoop


def pytest_sessionstart(session: Session) -> None:
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """
    asyncio.set_event_loop_policy(EventLoopPolicy())
