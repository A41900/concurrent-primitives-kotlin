# Concurrent Primitives Kotlin

A collection of concurrent programming primitives and synchronization constructs implemented in Kotlin.

This repository was developed as part of the Concurrent Programming course at ISEL and focuses on the design and implementation of thread-safe data structures, synchronization mechanisms, execution management abstractions, and publish-subscribe messaging systems.

---

## Components

### Queue

#### ThreadSafeNonBlockingQueue

Thread-safe unbounded FIFO queue with:

* non-blocking retrieval semantics (`removeOrNull`)
* weakly consistent iteration
* lock-based synchronization using `ReentrantLock`

---

### Synchronization

#### AutoResetLatch

Custom synchronization primitive inspired by auto-reset events.

Threads can wait for a signal through `await(timeout)`.
A call to `set()` releases all threads that were waiting at that specific moment and returns the number of released threads.

This implementation uses a generation-based design instead of storing one request object per waiting thread. It tracks:

- `activeAwaits` — threads currently inside `await`
- `setCounter` — generation counter used to distinguish old and new `set` calls

##### Features

- timeout-aware waiting
- JVM interrupt protocol support
- automatic reset semantics
- generation-based synchronization
- exact accounting of successful wakeups
- no accumulated permits for future awaits
- thread-safe implementation using `ReentrantLock` and `Condition`

---

### Execution Management

#### ThreadPool

Custom thread pool executor implemented in Kotlin.

This component executes `Continuation<Unit>` work items on a managed set of worker threads. The pool dynamically grows between a configured minimum and maximum number of workers, keeps idle workers alive for a limited time, and supports graceful shutdown.

##### Features

- executes `Continuation<Unit>` tasks
- configurable minimum and maximum worker count
- on-demand worker creation
- queued work when all workers are busy
- idle worker timeout using `keepAliveTime`
- graceful shutdown
- rejection of new work after shutdown
- `awaitTermination(timeout)` support
- worker survival after task exceptions
- suspending `invoke` extension that resumes the caller without blocking its thread

### Coroutine integration

The `invoke` extension function provides a suspending API over the thread pool.

It captures the caller coroutine continuation, submits work to the pool, executes the provided function on a worker thread, and resumes the original coroutine with either the returned result or the thrown exception.

This allows code to wait for work submitted to the pool without blocking the calling thread.


### BoundedThreadScope

`BoundedThreadScope` is a structured thread management abstraction.

It groups related threads and child scopes under a shared lifecycle, enforcing a maximum number of concurrently alive threads. Threads submitted beyond the configured limit are queued and started automatically as active threads complete.

### Features

- bounded number of concurrently alive threads
- queued thread startup when capacity is full
- child scope creation
- hierarchical lifecycle management
- idempotent close
- cancellation through interruption
- join with timeout
- parent notification when child scopes complete

### Design

The scope has three states:

- `OPEN` — accepts new threads and child scopes
- `CLOSING` — rejects new work and waits for active threads/children
- `CLOSED` — all started threads and child scopes have completed

Each thread is wrapped so that, when it finishes, the scope can start the next queued thread and check whether the scope has completed.
---

### Messaging

#### BoundedEventTopic

## BoundedEventTopic

`BoundedEventTopic<T>` is a thread-safe bounded publish-subscribe component.

It allows producers to publish events and consumers to create independent subscriptions. Each subscription keeps its own read position, so multiple subscribers can consume the same published events independently.

Events are stored in a fixed-size circular buffer. When the buffer reaches its maximum capacity, the oldest event is discarded. If a subscriber falls behind and tries to read an event that has already been overwritten, it is automatically advanced to the oldest event still available.

### Features

- bounded circular buffer
- multiple publishers
- multiple independent subscribers
- per-subscription read position
- timeout-aware blocking reads
- JVM interrupt support
- graceful close semantics
- automatic fast-forward for slow subscribers

#### Coroutine-based BoundedEventTopic

Coroutine-oriented version of the event topic using suspendable operations.

Features:

* suspendable publish/read operations
* non-blocking waiting
* coroutine-friendly API

---

## Testing

The repository includes:

* functional tests
* concurrency tests
* stress tests

used to validate correctness under concurrent workloads.

---

## Technologies

* Kotlin
* JVM Concurrency
* Coroutines
* JUnit

---

## Academic Context

This repository was developed collaboratively as part of the Concurrent Programming course at ISEL.

The objective was to explore the implementation of concurrent data structures, synchronization primitives, thread management abstractions, and asynchronous communication mechanisms.


## Components

- [ThreadSafeNonBlockingQueue](src/main/kotlin/ThreadSafeNonBlockingQueue.kt)
- [AutoResetLatch](src/main/kotlin/AutoResetLatch.kt)
- [ThreadPool](src/main/kotlin/ThreadPool.kt)
- [BoundedThreadScope](src/main/kotlin/BoundedThreadScope.kt)
- [BoundedEventTopic](src/main/kotlin/BoundedEventTopic.kt)
- [Coroutine BoundedEventTopic](src/main/kotlin/KBoundedEventTopic.kt)