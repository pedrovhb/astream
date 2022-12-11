from __future__ import annotations
from typing import *


_T = TypeVar("_T")


class StreamProperty(Generic[_T]):
    def __set_name__(self, instance: _T | None, owner: Type[_T] | None) -> _T:
        return instance._stream
