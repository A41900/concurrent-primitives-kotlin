import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class UnitTestThreadScope {
    @Test
    fun `single-thread scope`() {
        // given: a threadScope with maxAliveThreads = 1 and no threads created
        val t = BoundedThreadScope("thread1", Thread.ofPlatform(), 1)

        // when: closing the scope
        t.close()

        // then: join returns true immediately since there is nothing to wait for
        assertTrue(t.join(Duration.ofSeconds(5)))
    }

    @Test
    fun `single-thread scope that increments a counter`() {
        // given: a threadScope with maxAliveThreads = 1 and no threads created
        val t = BoundedThreadScope("thread2", Thread.ofPlatform(), 1)

        // and: a counter initialized with 0
        var counter = 0

        // when: creating a thread and incrementing the counter
        t.newThread {
            counter++
        }

        // and: closing the scope
        t.close()

        // then: join returns true and the counter was incremented
        assertTrue(t.join(Duration.ofSeconds(5)))
        assertEquals(1, counter)
    }

    @Test
    fun `when scope is full, new thread is created but not started`() {
        // given: a threadScope with maxAliveThreads = 1
        val t = BoundedThreadScope("thread3", Thread.ofPlatform(), 1)

        // and: a thread already running
        t.newThread {
            Thread.sleep(200)
        }

        // when: creating a second thread while the scope is full
        val thread2 =
            t.newThread {
                println("hello, im thread2")
            }

        // then: the second thread is created but not started
        assertEquals(Thread.State.NEW, thread2?.state)
        // Thread.State.NEW é o estado em que uma thread fica quando foi criada, mas não foi iniciada

        t.cancel()
        assertTrue(t.join(Duration.ofSeconds(5)))
    }

    @Test
    fun `when scope is not full, create new thread and run it`() {
        // given: a threadScope
        val t = BoundedThreadScope("root", Thread.ofPlatform(), 2)

        // and: a count variable
        var count = 0

        // when: creating a new thread and changing the variable
        val newT =
            t.newThread {
                count = 10
            }

        t.close()
        assertNotNull(newT)
        assertTrue(t.join(Duration.ofSeconds(2)))

        // then: it runs and changes the variable
        assert(count == 10)
    }

    @Test
    fun `when first thread finishes, waiting thread is started`() {
        // given: a threadScope with maxAliveThreads = 1
        val t = BoundedThreadScope("thread4", Thread.ofPlatform(), 1)

        // and: a counter initialized with 0
        var counter = 0

        // when: creating a first thread that sleeps
        t.newThread {
            Thread.sleep(200)
            counter++
        }

        // and: creating a second thread while the scope is full
        val thread2 =
            t.newThread {
                counter++
            }

        // then: the second thread is not yet started
        assertEquals(Thread.State.NEW, thread2?.state)

        // and: closing the scope
        t.close()

        // and: waiting for all threads to finish
        assertTrue(t.join(Duration.ofSeconds(5)))

        // then: both threads ran and the counter is 2
        assertEquals(2, counter)
    }

    @Test
    fun `when cancel is called, waiting threads are not started`() {
        // given: a threadScope with maxAliveThreads = 1
        val t = BoundedThreadScope("thread5", Thread.ofPlatform(), 1)

        // and: a counter
        var counter = 0

        // and: a first thread already running
        t.newThread {
            Thread.sleep(60000)
        }

        // and: a second thread waiting in the queue
        val thread2 =
            t.newThread {
                counter++
            }

        // when: cancelling the scope
        t.cancel()
        Thread.sleep(100)

        // then: the waiting thread was never started
        assertEquals(Thread.State.NEW, thread2?.state)
        assertEquals(0, counter)

        assertTrue(t.join(Duration.ofSeconds(2)))
    }

    @Test
    fun `when cancel is called, all threads are interrupted`() {
        // given: a threadScope with maxAliveThreads = 1
        val t = BoundedThreadScope("thread6", Thread.ofPlatform(), 1)

        // and: a thread that sleeps for a long time
        val thread1 =
            t.newThread {
                Thread.sleep(1000)
            }

        // when: cancelling the scope
        t.cancel()

        // then: join returns true because the thread was interrupted
        assertTrue(t.join(Duration.ofSeconds(3)))

        Thread.sleep(1000)
        // and: the thread is terminated
        assertEquals(Thread.State.TERMINATED, thread1?.state)
    }

    @Test
    fun `when parent scope joins, it waits for child scope to complete`() {
        // given: a parent scope and a child scope
        val parent = BoundedThreadScope("parent", Thread.ofPlatform(), 1)
        val child = parent.newChildScope("child")

        // and: a counter initialized with 0
        var counter = 0

        // when: creating a thread in the child scope that increments the counter
        child?.newThread {
            counter++
        }

        // and: closing both scopes
        child?.close()
        parent.close()

        // then: parent join returns true and the counter was incremented
        assertTrue(parent.join(Duration.ofSeconds(2)))
        assertEquals(1, counter)
    }

    @Test
    fun `when parent scope is cancelled, child scopes are also cancelled`() {
        // given: a parent scope and a child scope
        val parent = BoundedThreadScope("parent2", Thread.ofPlatform(), 1)
        val child = parent.newChildScope("child2")

        // and: a thread in the child scope that sleeps for a long time
        child?.newThread {
            Thread.sleep(60000)
        }

        // when: cancelling the parent scope
        parent.cancel()

        // then: join returns true because the child scope was also cancelled
        assertTrue(parent.join(Duration.ofSeconds(2)))
    }

    @Test
    fun `when performing join all thread are terminated`() {
        // given: a thread scope with capacity for 3 threads

        val t = BoundedThreadScope("root", Thread.ofPlatform(), 3)
        val threads = ConcurrentLinkedQueue<Thread>()

        // when: all threads execute a task
        repeat(3) {
            threads +=
                t.newThread {
                    Thread.sleep(100)
                }
        }

        // and: all of them finish the work
        t.join(Duration.ofMillis(200))

        // then: all threads are terminated
        threads.forEach {
            assertTrue(it.state == Thread.State.TERMINATED)
        }
    }

    @Test
    fun `parent completes when empty child scope is closed`() {
        val parent = BoundedThreadScope("parent", Thread.ofPlatform(), 1)
        val child = parent.newChildScope("child")!!

        child.close()
        parent.close()

        assertTrue(parent.join(Duration.ofSeconds(1)))
    }
}

class StressTestThreadScope {
    @Test
    fun `alive threads never exceed maxAliveThreads`() {
        // given: a threadScope with maxAliveThreads = 4
        val nOfThreads = 8
        val numOfReps = 100_000

        val maxAliveThreads = 4
        val boundedThreadScope = BoundedThreadScope("thread", Thread.ofPlatform(), maxAliveThreads)

        // and: a counter to track alive threads
        val counter = AtomicInteger(0)

        // when: multiple threads concurrently submit tasks to the scope
        TestHelper(Thread.ofPlatform()).runTest { testHelper ->
            repeat(nOfThreads) {
                testHelper.startThread {
                    repeat(numOfReps) {
                        boundedThreadScope.newThread {
                            // then: the number of alive threads never exceeds the maximum
                            val observed = counter.incrementAndGet()
                            assertTrue(observed <= maxAliveThreads)
                            Thread.yield()
                            counter.decrementAndGet()
                        }
                    }
                }
            }
        }
    }

    @Test
    fun `all threads are executed`() {
        // given: a threadScope with maxAliveThreads = 4
        val nOfThreads = 8
        val numOfReps = 1000

        val maxAliveThreads = 4
        val boundedThreadScope = BoundedThreadScope("thread", Thread.ofPlatform(), maxAliveThreads)

        // and: a counter to track executed tasks
        val counter = AtomicInteger(0)

        // when: multiple threads concurrently submit tasks to the scope
        TestHelper(Thread.ofPlatform()).runTest { testHelper ->
            repeat(nOfThreads) {
                testHelper.startThread {
                    repeat(numOfReps) {
                        boundedThreadScope.newThread {
                            counter.incrementAndGet()
                        }
                    }
                }
            }
        }

        // and: closing the scope and waiting for all tasks to finish
        boundedThreadScope.close()
        assertTrue(boundedThreadScope.join(Duration.ofSeconds(5)))

        // then: all tasks were executed
        assertEquals(nOfThreads * numOfReps, counter.get())
    }

    @Test
    fun `cancel stops all running threads`() {
        // given: a threadScope with maxAliveThreads = 4
        val nOfThreads = 8
        val numOfReps = 1000

        val maxAliveThreads = 4
        val boundedThreadScope = BoundedThreadScope("thread", Thread.ofPlatform(), maxAliveThreads)

        // when: multiple threads concurrently submit long-running tasks to the scope
        TestHelper(Thread.ofPlatform()).runTest { testHelper ->
            repeat(nOfThreads) {
                testHelper.startThread {
                    repeat(numOfReps) {
                        boundedThreadScope.newThread {
                            Thread.sleep(60000)
                        }
                    }
                }
            }

            // and: cancel is called while threads are still running
            testHelper.startThread {
                Thread.sleep(100)
                boundedThreadScope.cancel()
            }
        }

        // then: join returns true because all threads were cancelled
        assertTrue(boundedThreadScope.join(Duration.ofSeconds(2)))
    }

    @Test
    fun `when close is called, no new threads are created after it`() {
        // given: a threadScope with maxAliveThreads = 4
        val nOfThreads = 8
        val numOfReps = 1000

        val maxAliveThreads = 4
        val boundedThreadScope = BoundedThreadScope("thread", Thread.ofPlatform(), maxAliveThreads)

        // when: multiple threads concurrently submit tasks to the scope
        TestHelper(Thread.ofPlatform()).runTest { testHelper ->
            repeat(nOfThreads) {
                testHelper.startThread {
                    repeat(numOfReps) {
                        boundedThreadScope.newThread {
                            print("Hello")
                        }
                    }
                }
            }

            // and: close is called after a small delay
            testHelper.startThread {
                Thread.sleep(100)
                boundedThreadScope.close()

                // then: newThread returns null after close
                val thread = boundedThreadScope.newThread { }
                assertNull(thread)
            }
        }

        // and: join returns true because all threads completed
        assertTrue(boundedThreadScope.join(Duration.ofSeconds(2)))
    }

    @Test
    fun `when performing join on a scope with multiple child scopes all of them finish`() {
        // given: a thread scope
        val scope = BoundedThreadScope("root", Thread.ofPlatform(), 3)
        val helper = TestHelper(Thread.ofPlatform())
        val finish = ConcurrentLinkedQueue<Thread>()

        // and: multiple nested child scopes
        val level1 = scope.newChildScope("child1")!!
        val level2 = level1.newChildScope("child2")!!
        val level3 = level2.newChildScope("child3")!!

        // when:
        repeat(2) {
            helper.startThread {
                finish +=
                    level1.newThread {
                        Thread.sleep(100)
                    }
            }
            helper.startThread {
                finish +=
                    level2.newThread {
                        Thread.sleep(100)
                    }
            }
            helper.startThread {
                finish +=
                    level3.newThread {
                        Thread.sleep(100)
                    }
            }
        }

        scope.close()

        // and: closing waiting for all scopes to be finshed
        scope.join(Duration.ofMillis(1000))

        // then: all threads are terminated
        finish.forEach {
            assertTrue(it.state == Thread.State.TERMINATED)
        }
    }

    @Test
    fun `max alive threads is never exceeded`() {
        val maxAlive = 16

        // given: a thread scope with a max alive thread limit
        val scope = BoundedThreadScope("root", Thread.ofPlatform(), maxAlive)
        val helper = TestHelper(Thread.ofPlatform())

        val mutex = ReentrantLock()
        var count = 0
        var max = 0

        fun inc() =
            mutex.withLock {
                count++
                if (count > max) {
                    max = count
                }
            }

        fun dec() = mutex.withLock { count-- }

        val nOfThreads = 1000

        helper.runTest {
            repeat(nOfThreads) {
                // when: creating new threads
                scope.newThread {
                    inc()

                    try {
                        Thread.sleep(100)
                    } finally {
                        dec()
                    }
                }
            }
        }

        scope.close()
        val completed = scope.join(Duration.ofMillis(10000))

        // then: all threads are terminated and the max alive thread limit is never exceeded
        assertTrue(completed)
        assertEquals(0, count)
        assertTrue(max <= maxAlive)
    }
}
