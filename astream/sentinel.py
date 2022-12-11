from __future__ import annotations

import enum
from typing import Literal, TypeAlias


class Sentinel(enum.Enum):
    NoValue = enum.auto()
    Skip = enum.auto()
    Stop = enum.auto()
    RaiseException = enum.auto()


_SkipT: TypeAlias = Literal[Sentinel.Skip]
_StopT: TypeAlias = Literal[Sentinel.Stop]
_NoValueT: TypeAlias = Literal[Sentinel.NoValue]
_RaiseExceptionT: TypeAlias = Literal[Sentinel.RaiseException]
__all__ = ("Sentinel",)
