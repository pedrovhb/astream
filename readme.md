# asyncutils

`asyncutils` is a library which provides handy tools for writing code with `asyncio`. It's designed with static type checking in mind, so type annotations are optional but fully supported.

The library is currently in a very early stage, and features are as of yet unstable (i.e. subject to API changes) and not extensively tested.

## Features

### Async iterator utilites

#### `merge_async_iterators`, aka `amerge`

Transforms multiple asynchronous iterators into a single one, yielding items from all of them as they come in.

```python
a1 =  arange(10)
a2 = arange(90, 100)
a3 = arange(1337, 1348)

async for item in amerge(a1, a2, a3):
    print(item)  # 10, 90, 1337, ...
```

#### `tee_async_iterators`, aka `atee`

Transform an async iterable into multiple async iterators which produce the same items.

```python
source = arange(10)
a1, a2 = atee(source, 2)

async for item in a1:
    print(item)  # 0, 1, 2...

async for item in a2:
    print(item)  # 0, 1, 2, ...
```

Note that *the original async iterator should not be used after teeing*. Doing so will consume items and they won't be seen by the tee, and won't be distributed to the derivate async iterators.

`atee` works well when you know exactly how many async iterators you'll need. If there's a need for on-demand copies, check out `aclone`.

#### `clone_async_iterable`, aka `aclone`

Takes an asynchronous iterable or iterator and returns one with the `aclone()` method:
(todo - example)

`aclone` returns a transparent proxy of the provided object that adds the `aclone` method. This means that it works not only with async iterators, but also with any object that implements the `AsyncIterator` protocol, without interfering with its other attributes. Calling `aclone()` on the returned object returns the object itself, but asynchronously iterating over it won't consume items from other clones:

```python
# todo - flesh out example
class MyClass(AsyncIterator[T]):
    ...
    async def __anext__(self) -> AsyncIterator[T]:
        return await self.apop()

original = MyClass()
original_clone = aclone(MyClass())
clone_1 = original_clone.aclone()

original_clone.foo = "bar"
print(clone_1.foo)  # bar

original_clone.foo = "fizz"
print(clone_1.foo)  # fizz


async for item in clone_1:
    print(item)  # 1, 2, 3

# Items won't have been consumed from other clones
async for item in original_clone:
    print(item)  # 1, 2, 3
# Further clones can be created from either the first one or subsequent ones
clone_2 = clone_1.aclone()

# todo - verify behavior for the original object
# todo - consider providing mixin class
```

- todo - implement same model for afilter, amap
	- todo - atransform? more general operation which doesn't necessarily map 1:1 - comprehends amap, a filter, aflatmap, etc.

### CloseableQueue

Standard `asyncio` queues are great for managing data flow, but there's a bit of a mismatch when combining them with async iterators - queues always live forever, and iterators don't necessarily. Standard queues can be joined, but that only ensures that they're empty at some moment in time, and not that new items won't be subsequently added. `asyncutils` provides closeable queue types which help building finite pipelines with backpressure:

```python
queue = CloseableQueue(maxsize=5)
# ... Todo - example
```

A CloseableQueue accepts items via `put` and `put_nowait` and provides items via `get` and `get_nowait`, same as the standard library queues.
When the queue is closed by calling its `close` method, it will no longer accept new items (attempting to do so will raise `QueueClosed`), but will continue to provide items until it's empty. Once it's empty, it will raise `QueueExhausted` on `get` and `get_nowait` calls. It's possible to check whether a queue is closed or exhausted with `queue.is_closed` and `queue.is_exhausted`, and to asynchronously wait until a queue is closed with `await queue.wait_closed()` and `await queue.wait_exhausted()`.

Closeable queues can be iterated upon. The iteration will finish when the queue is exhausted:

```python
queue = CloseableQueue(maxsize=5)

async def fill_queue() -> None:
    for i in range(10):
        await queue.put(i)
    queue.close()
asyncio.create_task(fill_queue())

async for item in queue:
    print(item)  # 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
```


```python
# Type annotations optional, but recommended
queue: CloseableQueue[int] = CloseableQueue()
# ...
async def producer(q):
    async for i in arange(10):
		await q.put(i)
	q.close()

# Create
asyncio.create_task(producer(queue))
async for item in q2a(queue):
    print(item)
    # 0, 1, 2, ...

print("Done!")
# With a standard Queue, this iteration would never end
```

`asyncutils` also provides closeable versions of the other standard library queues - namely, `CloseablePriorityQueue` and `CloseableLifoQueue`.

## Planned/work in progress features

### ReactiveProperty

A descriptor which allows asynchronously iterating over changes, or setting callbacks to be run whenever a change occurs:

(Todo - example)

It's possible to iterate over a reactive property's changes to specific instances, or to any instance using the property:

(Todo - example over class-wide ReactiveProperty)

(Todo - example with ReactiveProperty created off-class and reused in different classes?)

### WorkerPool

Distribution of work across multiple workers, with support for backpressure and cancellation.

<details>
 <summary>Self-notes</summary>

- Todo - explore an API for populating queues from async iterators. Considerations:
	- Could be either an external function, or a function in CloseableQueue itself. If an external function, it could also be used for populating standard library Queues.
	- It'd be quite useful for composed/functional pipelines to have the option to close the queue after the async iterator is done.
		- This could be a separate function (`populate_and_close`) or a parameter `populate_queue(aiter, close_when_done=True)`.
			- The parameter wouldn't make sense for standard lib queues
		- What happens when two async iterators are set to populate the queue and then close it?
			- Most sane thing would be to wait until both are finished, then close it
				- Easy to do cleanly if it's a method of the CloseableQueue, not so much if it's a standalone function - requires keeping global state
		- Possibility
			- Have both a simple `populate_queue` standalone function with no closing functionality which works for both standard queues and closeable ones, and have a `populate` method in closeable queues which takes the `close_when_done` param

</details>
