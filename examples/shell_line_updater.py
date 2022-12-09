#! /usr/bin/env python
import os

from astream.stream_utils import delay, interleave_with, repeat_value
from astream.utils import run_stream
from astream.sources import from_stdin_raw
from astream.sinks import to_stdout

if __name__ == "__main__":

    terminal_width = os.get_terminal_size().columns

    # Print the input replacing lines by interleaving with spaces and \r
    result = run_stream(
        repeat_value(b" " * terminal_width)
        / delay(0.5)
        / interleave_with(from_stdin_raw(), stop_on_first_empty=True)
        / to_stdout(line_separator=b"\r")
    )
