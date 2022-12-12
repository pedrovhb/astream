import asyncio
from asyncio import Future
from typing import Callable, AsyncIterator, TypeVar, AsyncIterable

from astream.closeable_queue import CloseableQueue
from astream.stream import Transformer, Stream
from astream.stream_utils import arange_delayed, delay, arange

_I = TypeVar("_I")
_O = TypeVar("_O")


class WorkerQueue(Transformer[_I, _O]):
    """A stream that will run the given transformer function in a worker pool.

    Notes:
        - We use two CloseableQueues between the class. The first is used to feed the workers
            with items and the second is used to collect the results from the workers. We use two
            queues because we want to be able to yield the items in the same order they were
            received. The output queue contains futures that are inserted in the same order as they
            arrive, and the output gatherer awaits each of them.
    """

    def __init__(
        self, transformer_fn: Callable[[AsyncIterator[_I]], Transformer[_I, _O]], n_workers: int = 5
    ) -> None:
        super().__init__()
        self._in_q: CloseableQueue[tuple[_I, Future[_O]]] = CloseableQueue()
        self._out_q: CloseableQueue[Future[_O]] = CloseableQueue()

        self._n_workers = n_workers
        self._transformer_fn = transformer_fn

        self._started_ev = asyncio.Event()
        self._workers = None

    async def _worker_feeder(self, src: AsyncIterable[_I]) -> None:
        loop = asyncio.get_event_loop()
        async for item in src:
            item_fut: Future[_O] = loop.create_future()
            self._out_q.put_nowait(item_fut)
            await self._in_q.put((item, item_fut))
        self._in_q.close()
        self._out_q.close()

    async def _output_gatherer(self) -> AsyncIterator[_O]:
        async for fut in self._out_q:
            yield await fut

    def _create_workers(self) -> None:
        for _ in range(self._n_workers):
            asyncio.create_task(self._worker())

    async def _worker(self) -> None:
        worker_q: CloseableQueue[_I] = CloseableQueue(maxsize=1)
        transformer = Stream(aiter(worker_q)).transform(self._transformer_fn)
        async for item, fut in self._in_q:
            await worker_q.put(item)
            fut.set_result(await anext(transformer))
        worker_q.close()

    def transform(self, src: AsyncIterable[_I]) -> AsyncIterator[_O]:
        if not self._started_ev.is_set():
            self._started_ev.set()
            asyncio.create_task(self._worker_feeder(src))
            self._create_workers()
        return self._output_gatherer()


if __name__ == "__main__":

    async def main() -> None:
        async for item in arange(20) / WorkerQueue(delay(1)):
            print(item)

    asyncio.run(main(), debug=True)
