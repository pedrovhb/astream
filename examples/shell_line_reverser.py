#! /usr/bin/env python


from astream.stream_utils import delay, from_stdin, to_stdout
from astream.utils import run_stream


if __name__ == "__main__":

    run_stream(
        from_stdin()
        .transform(delay(0.01))
        .transform(bytes.upper)
        .transform(lambda s: s[::-1])
        .transform(to_stdout())
    )
