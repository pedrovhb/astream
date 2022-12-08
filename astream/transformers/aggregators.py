from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import TypeVar, AsyncIterator, cast

from stream import transformer, Stream

_T = TypeVar("_T")
