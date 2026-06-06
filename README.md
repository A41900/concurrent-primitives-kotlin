# Concurrent Primitives Kotlin

A collection of concurrent programming primitives and synchronization constructs implemented in Kotlin.

This repository was developed as part of the Concurrent Programming course at ISEL and focuses on the design and implementation of thread-safe data structures, synchronization mechanisms, execution management abstractions, and publish-subscribe messaging systems.

---

# Table of Contents

1. [Overview](#overview)
2. [Components](#components)
    - [ThreadSafeNonBlockingQueue](#threadsafenonblockingqueue)
    - [AutoResetLatch](#autoresetlatch)
    - [ThreadPool](#threadpool)
    - [BoundedThreadScope](#boundedthreadscope)
    - [BoundedEventTopic](#boundedeventtopic)
    - [Coroutine BoundedEventTopic](#coroutine-boundedeventtopic)
3. [Testing](#testing)
4. [Technologies](#technologies)
5. [Project Structure](#project-structure)
6. [Academic Context](#academic-context)

---

# Overview

This repository contains a collection of concurrent programming primitives and synchronization constructs implemented in Kotlin.

The project explores:

- thread-safe data structures
- synchronization primitives
- execution management abstractions
- publish-subscribe messaging systems
- coroutine integration

---

# Components

## ThreadSafeNonBlockingQueue

Thread-safe unbounded FIFO queue with:

- non-blocking retrieval semantics (`removeOrNull`)
- weakly consistent iteration
- lock-based synchronization using `ReentrantLock`

---

## AutoResetLatch

Custom synchronization primitive inspired by auto-reset events.

Threads can wait for a signal through `await(timeout)`.
A call to `set()` releases all threads that were waiting at that specific moment and returns the number of released threads.

This implementation uses a generation-based design instead of storing one request object per waiting thread. It tracks:

- `activeAwaits` — threads currently inside `await`
- `setCounter` — generation counter used to distinguish old and new `set` calls


---

## ThreadPool

Custom thread pool executor implemented in Kotlin.

This component executes `Continuation<Unit>` work items on a managed set of worker threads. The pool dynamically grows between a configured minimum and maximum number of workers, keeps idle workers alive for a limited time, and supports graceful shutdown.

### Features

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

---

## BoundedThreadScope

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


Thread-safe bounded publish-subscribe component.

It allows producers to publish events while consumers create independent subscriptions.

Each subscription maintains its own read position, allowing multiple subscribers to consume the same events independently.

### Features

- bounded circular buffer
- multiple publishers
- multiple subscribers
- per-subscription read cursor
- timeout-aware blocking reads
- JVM interrupt support
- graceful close semantics
- automatic fast-forward for lagging subscribers

---

# Testing

The repository includes:

- functional tests
- concurrency tests
- stress tests

used to validate correctness under concurrent workloads.

---

# Technologies

- Kotlin
- JVM Concurrency
- Coroutines
- JUnit

---

# Project Structure

```text
src/main/kotlin/
├── ThreadSafeNonBlockingQueue.kt
├── AutoResetLatch.kt
├── ThreadPool.kt
├── BoundedThreadScope.kt
├── BoundedEventTopic.kt
└── KBoundedEventTopic.kt
```

---

# Academic Context

This repository was developed collaboratively as part of the Concurrent Programming course at ISEL.

The objective was to explore the implementation of:

- concurrent data structures
- synchronization primitives
- thread management abstractions
- asynchronous communication mechanisms

---


