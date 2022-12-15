import asyncio
from asyncio import Future
from typing import (
    Callable,
    AsyncIterator,
    TypeVar,
    AsyncIterable,
    AsyncGenerator,
    Protocol,
    Generic,
)

from astream.closeable_queue import CloseableQueue
from astream.stream import Transformer, Stream, FnTransformer
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
        transformer: Stream[_O] = Stream(aiter(worker_q)).transform(self._transformer_fn)
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


class MultiWorker(Generic[_I, _O]):
    def __init__(self, n_workers: int = 5) -> None:
        self._n_workers = n_workers

    def __rpow__(self, other: Transformer[_I, _O]) -> Transformer[_I, _O]:
        active_workers = self._n_workers

        out_queue: CloseableQueue[_O] = CloseableQueue()
        in_queue: CloseableQueue[_I] = CloseableQueue()

        async def worker() -> None:
            nonlocal active_workers
            async for item in other.transform(aiter(in_queue)):
                await out_queue.put(item)
            active_workers -= 1
            if active_workers == 0:
                out_queue.close()

        def _run() -> Stream[_O]:
            async def _inner(src: AsyncIterator[_I]) -> None:
                async for item in src:
                    await in_queue.put(item)
                in_queue.close()

            task_workers = [asyncio.create_task(worker()) for _ in range(self._n_workers)]
            return FnTransformer(_inner)

        return FnTransformer(_run)


if __name__ == "__main__":

    import asyncio

    async def main() -> None:
        # reveal_type(t)
        async for chunk in (arange(10) / (delay(1) ** MultiWorker(4))):
            print(chunk)

        # async for item in arange(20) / WorkerQueue(delay(1)):
        #     print(item)

    asyncio.run(main(), debug=True)
