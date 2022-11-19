from __future__ import annotations


from .utils import *
import astream.stream_utils as pure
from .stream import *


aconcatenate = stream(pure.aconcatenate)
aenumerate = stream(pure.aenumerate)
afilter = stream(pure.afilter)
aflatmap = stream(pure.aflatmap)
aflatten = stream(pure.aflatten)
agetattr = stream(pure.agetattr)
agetitem = stream(pure.agetitem)
amap = stream(pure.amap)
amerge = stream(pure.amerge)
arange = stream(pure.arange)
arange_delayed = stream(pure.arange_delayed)
arange_delayed_random = stream(pure.arange_delayed_random)
arange_delayed_sine = stream(pure.arange_delayed_sine)
arepeat = stream(pure.arepeat)
ascan = stream(pure.ascan)
atee = stream(pure.atee)
azip = stream(pure.azip)
azip_longest = stream(pure.azip_longest)
bytes_stream_split_separator = stream(pure.bytes_stream_split_separator)
with_exc_handler = stream(pure.with_exc_handler)

from . import experimental
from .worker_pool import *
from .stream_grouper import *
