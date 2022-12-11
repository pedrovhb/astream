#! /usr/bin/env python


from astream.sinks import to_stdout
from astream.sources import from_stdin_raw
from astream.stream_utils import delay


async def main() -> None:

    await (
        from_stdin_raw()
        .transform(delay(0.01))
        .transform(bytes.upper)
        .transform(lambda s: s[::-1])
        .transform(to_stdout())
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
