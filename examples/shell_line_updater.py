#! /usr/bin/env python
import os

from astream.stream_utils import delay, from_stdin, interleave_with, repeat_value, to_stdout
from astream.utils import run_stream

if __name__ == "__main__":

    terminal_width = os.get_terminal_size().columns

    # Print the input replacing lines by interleaving with spaces and \r
    result = run_stream(
        repeat_value(b" " * terminal_width)
        / delay(0.5)
        / interleave_with(from_stdin(), stop_on_first_empty=True)
        / to_stdout(line_separator=b"\r")
    )
