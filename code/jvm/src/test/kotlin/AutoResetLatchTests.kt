import java.time.Duration
import java.util.Collections
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class UnitAutoRequestLatchTest {
    @Test
    fun `await returns false when timeout is zero`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()

        // when: await is called with zero timeout
        val result = resetLatch.await(Duration.ZERO)

        // then: the result is false
        assertFalse(result)
    }

    @Test
    fun `await returns false when timeout expires`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()

        // when: await is called with a short timeout and no set is called
        val result = resetLatch.await(Duration.ofSeconds(1))

        // then: the result is false
        assertFalse(result)
    }

    @Test
    fun `await returns true when set is called`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()
        var result = false

        // and: a thread blocked on await
        val thread =
            Thread {
                result = resetLatch.await(Duration.ofSeconds(5))
            }
        thread.start()
        Thread.sleep(100)

        // when: set is called
        resetLatch.set()
        thread.join()

        // then: the blocked thread returns true
        assertTrue(result)
    }

    @Test
    fun `set returns number of awaiting threads`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()

        // and: 3 threads blocked on await
        val thread =
            Thread {
                resetLatch.await(Duration.ofSeconds(5))
            }
        thread.start()

        val thread2 =
            Thread {
                resetLatch.await(Duration.ofSeconds(5))
            }
        thread2.start()

        val thread3 =
            Thread {
                resetLatch.await(Duration.ofSeconds(5))
            }
        thread3.start()

        Thread.sleep(100)

        // when: set is called
        val set = resetLatch.set()

        thread.join()
        thread2.join()
        thread3.join()

        // then: set returns 3
        assertEquals(3, set)
    }

    @Test
    fun `set returns zero when no threads are waiting`() {
        // given: an AutoResetLatch and 0 threads blocked on await
        val resetLatch = AutoResetLatch()

        // when: set is called()
        val set = resetLatch.set()

        // then: set returns 0
        assertEquals(0, set)
    }

    @Test
    fun `auto reset - set has no effect on future awaits`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()

        // and: a call of set before any await
        resetLatch.set()

        // when: await is called after the set
        val result = resetLatch.await(Duration.ofSeconds(5))

        // then: returns false because the previous set has no effect on future awaits false
        assertFalse(result)
    }

    @Test
    fun `await throws InterruptedException when interrupted`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()
        var isInterrupted = false

        // and: a thread blocked on await
        val thread =
            Thread {
                try {
                    resetLatch.await(Duration.ofSeconds(5))
                } catch (e: InterruptedException) {
                    isInterrupted = true
                }
            }
        thread.start()
        Thread.sleep(100)

        // when: the thread is interrupted
        thread.interrupt()
        thread.join()

        // then: the thread caught an InterruptedException
        assertTrue(isInterrupted)
    }

    @Test
    fun `interrupted thread does not count in set return value`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()

        var thread1Interrupted = false

        // and: 2 threads blocked on await
        val thread1 =
            Thread {
                try {
                    resetLatch.await(Duration.ofSeconds(5))
                } catch (e: InterruptedException) {
                    thread1Interrupted = true
                }
            }

        val thread2 =
            Thread {
                resetLatch.await(Duration.ofSeconds(5))
            }

        thread1.start()
        thread2.start()
        Thread.sleep(100)

        // when: thread1 is interrupted
        thread1.interrupt()
        Thread.sleep(100)

        // and: set is called
        val result = resetLatch.set()

        thread1.join()
        thread2.join()

        // then: set returns 1 because the interrupted thread does not count
        assertTrue(thread1Interrupted)
        assertEquals(1, result)
    }

    @Test
    fun `thread that enters after set must not be released by previous set`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()
        var firstResult = false

        // and: a thread blocked on await
        val t1 =
            Thread {
                firstResult = latch.await(Duration.ofSeconds(5))
            }

        t1.start()
        Thread.sleep(100)

        // when: set is called
        val released = latch.set()

        t1.join()

        // then: the waiting thread is released
        assertEquals(1, released)
        assertTrue(firstResult)

        // when: a new await is started after the set
        val secondResult = latch.await(Duration.ofMillis(100))

        // then: the previous set has no effect on future awaits
        assertFalse(secondResult)
    }

    @Test
    fun `if set returns N then N awaits return true`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()
        val n = 5

        // and: N threads that will wait on the latch
        val ready = CountDownLatch(n)
        val results = Collections.synchronizedList(mutableListOf<Boolean>())

        val threads =
            List(n) {
                Thread {
                    ready.countDown()
                    val result = latch.await(Duration.ofSeconds(5))
                    results.add(result)
                }
            }

        threads.forEach { it.start() }

        // and: all threads have started waiting
        assertTrue(ready.await(1, TimeUnit.SECONDS))
        Thread.sleep(100)

        // when: set is called
        val released = latch.set()

        threads.forEach { it.join(1_000) }

        // then: set returns N
        assertEquals(n.toLong(), released)

        // and: exactly N await calls return true
        assertEquals(n, results.count { it })

        // and: all waiting threads complete
        assertTrue(threads.none { it.isAlive })
    }

    @Test
    fun `set return value matches number of awaits returning true`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()
        val n = 10
        val ready = CountDownLatch(n)
        val results = Collections.synchronizedList(mutableListOf<Boolean>())

        // and: N threads blocked in await
        val threads =
            List(n) {
                Thread {
                    ready.countDown()
                    results.add(latch.await(Duration.ofSeconds(5)))
                }
            }

        threads.forEach { it.start() }
        assertTrue(ready.await(1, TimeUnit.SECONDS))
        Thread.sleep(100)

        // when: set is called
        val released = latch.set()

        threads.forEach { it.join(1_000) }

        // then: set returns N
        assertEquals(n.toLong(), released)

        // and: exactly N awaits returned true
        assertEquals(released, results.count { it }.toLong())

        // and: no thread remains blocked
        assertTrue(threads.none { it.isAlive })
    }

    @Test
    fun `timeout before set does not consume permission`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()

        // and: one await times out before any set
        val firstResult = latch.await(Duration.ofMillis(50))

        // when: set is called after timeout
        val released = latch.set()

        // then: timeout returned false
        assertFalse(firstResult)

        // and: set returns zero
        assertEquals(0, released)
    }

    @Test
    fun `sets without waiters do not allow future await to return true`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()

        // when: set is called with no waiting threads
        assertEquals(0, latch.set())
        assertEquals(0, latch.set())

        // and: await is called afterwards
        val result = latch.await(Duration.ofMillis(100))

        // then: previous sets have no effect
        assertFalse(result)
    }

    @Test
    fun `await returns false when timeout is negative`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()

        // when: await is called with a negative timeout
        val result = latch.await(Duration.ofMillis(-1))

        // then: the result is false
        assertFalse(result)
    }

    @Test
    fun `interrupted thread before set does not consume permission`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()
        var interrupted = false
        var secondResult = false

        // and: one thread blocked in await
        val t1 =
            Thread {
                try {
                    latch.await(Duration.ofSeconds(5))
                } catch (e: InterruptedException) {
                    interrupted = true
                }
            }

        t1.start()
        Thread.sleep(100)

        // when: the waiting thread is interrupted before set
        t1.interrupt()
        t1.join(1_000)

        // and: another thread waits afterwards
        val t2 =
            Thread {
                secondResult = latch.await(Duration.ofSeconds(5))
            }

        t2.start()
        Thread.sleep(100)

        val released = latch.set()
        t2.join(1_000)

        // then: the interrupted thread did not consume a future release
        assertTrue(interrupted)

        // and: set releases only the second thread
        assertEquals(1, released)
        assertTrue(secondResult)
        assertFalse(t1.isAlive)
        assertFalse(t2.isAlive)
    }

    @Test
    fun `timeout before set does not consume future release`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()
        var secondResult = false

        // and: one await times out before any set
        val firstResult = latch.await(Duration.ofMillis(50))

        // when: another thread waits afterwards
        val t2 =
            Thread {
                secondResult = latch.await(Duration.ofSeconds(5))
            }

        t2.start()
        Thread.sleep(100)

        val released = latch.set()
        t2.join(1_000)

        // then: the first await returned false
        assertFalse(firstResult)

        // and: set releases only the second await
        assertEquals(1, released)
        assertTrue(secondResult)
        assertFalse(t2.isAlive)
    }

    @Test
    fun `two consecutive sets release different groups of waiters`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()

        val firstResults = Collections.synchronizedList(mutableListOf<Boolean>())
        val secondResults = Collections.synchronizedList(mutableListOf<Boolean>())

        // and: first group of waiters
        val firstGroup =
            List(3) {
                Thread {
                    firstResults.add(latch.await(Duration.ofSeconds(5)))
                }
            }

        firstGroup.forEach { it.start() }
        Thread.sleep(100)

        // when: set is called for the first group
        val firstReleased = latch.set()
        firstGroup.forEach { it.join(1_000) }

        // and: second group starts waiting after the first set
        val secondGroup =
            List(2) {
                Thread {
                    secondResults.add(latch.await(Duration.ofSeconds(5)))
                }
            }

        secondGroup.forEach { it.start() }
        Thread.sleep(100)

        // and: set is called for the second group
        val secondReleased = latch.set()
        secondGroup.forEach { it.join(1_000) }

        // then: first set released only the first group
        assertEquals(3, firstReleased)
        assertEquals(3, firstResults.count { it })

        // and: second set released only the second group
        assertEquals(2, secondReleased)
        assertEquals(2, secondResults.count { it })

        // and: no thread remains blocked
        assertTrue(firstGroup.none { it.isAlive })
        assertTrue(secondGroup.none { it.isAlive })
    }

    @Test
    fun `set called after released waiter completed does not release future await`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()
        var firstResult = false

        // and: one thread blocked in await
        val t1 =
            Thread {
                firstResult = latch.await(Duration.ofSeconds(5))
            }

        t1.start()
        Thread.sleep(100)

        // when: set is called
        val firstReleased = latch.set()
        t1.join(1_000)

        // and: set is called again with no waiters
        val secondReleased = latch.set()

        // and: a new await starts after both sets
        val futureResult = latch.await(Duration.ofMillis(100))

        // then: the first set released the original waiter
        assertEquals(1, firstReleased)
        assertTrue(firstResult)
        assertFalse(t1.isAlive)

        // and: the second set had no waiters to release
        assertEquals(0, secondReleased)

        // and: the future await is not released by the previous sets
        assertFalse(futureResult)
    }

    @Test
    fun `second set does not count waiters already released by first set`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()
        val n = 100

        // and: N threads waiting on await
        val ready = CountDownLatch(n)
        val results = Collections.synchronizedList(mutableListOf<Boolean>())

        val threads =
            List(n) {
                Thread {
                    ready.countDown()
                    results.add(latch.await(Duration.ofSeconds(5)))
                }
            }

        threads.forEach { it.start() }
        assertTrue(ready.await(1, TimeUnit.SECONDS))
        Thread.sleep(100)

        // when: set is called twice without new waiters
        val firstReleased = latch.set()
        val secondReleased = latch.set()

        threads.forEach { it.join(1_000) }

        // then: the first set releases all waiters
        assertEquals(n.toLong(), firstReleased)

        // and: the second set releases no additional waiters
        assertEquals(0, secondReleased)

        // and: exactly N awaits return true
        assertEquals(n, results.count { it })

        // and: no thread remains blocked
        assertTrue(threads.none { it.isAlive })
    }
}

class StressAutoResetLatchTest {
    @Test
    fun `multiple threads all released by single set`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()
        val threadResults = ConcurrentLinkedDeque<Boolean>()

        // and: 100 threads blocked on await
        val threads =
            (1..100).map {
                Thread {
                    threadResults.add(resetLatch.await(Duration.ofSeconds(10)))
                }
            }
        threads.forEach { it.start() }
        Thread.sleep(500)

        // when: set is called
        val result = resetLatch.set()

        threads.forEach { it.join() }

        // then: set returns 100
        assertEquals(100, result)
        // and: all threads return true
        assertTrue(threadResults.all { it })
    }

    @Test
    fun `repeated set and await cycles`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()

        repeat(100) {
            // and: 5 threads blocked on await
            val threadResults = ConcurrentLinkedDeque<Boolean>()
            val threads =
                (1..5).map {
                    Thread {
                        threadResults.add(resetLatch.await(Duration.ofSeconds(10)))
                    }
                }
            threads.forEach { it.start() }
            Thread.sleep(10)

            // when: set is called
            val result = resetLatch.set()
            threads.forEach { it.join() }

            // then: set returns 5 and all threads returned true
            assertEquals(5, result)
            assertTrue(threadResults.all { it })
        }
    }

    @Test
    fun `concurrent timeouts and set`() {
        // given: an AutoResetLatch
        val resetLatch = AutoResetLatch()
        val threadResults = ConcurrentLinkedDeque<Boolean>()

        // and: 100 threads with short timeouts
        val threads =
            (1..100).map {
                Thread {
                    threadResults.add(resetLatch.await(Duration.ofMillis(50)))
                }
            }
        threads.forEach { it.start() }
        Thread.sleep(25)

        // when: set is called while some threads may have already timed out
        val result = resetLatch.set()
        threads.forEach { it.join() }

        // then: the number of true results matches what set returned
        val trueCount = threadResults.count { it }
        assertEquals(trueCount.toLong(), result)
    }

    @Test
    fun `timed out thread does not count in set return value`() {
        // given: an AutoResetLatch
        val latch = AutoResetLatch()

        // and: a thread waiting with a short timeout
        val t1 =
            Thread {
                latch.await(Duration.ofMillis(100))
            }

        t1.start()
        t1.join()

        // when: set is called after the timeout
        val released = latch.set()

        // then: the timed out thread does not count
        assertEquals(0, released)
    }

    @Test
    fun `multiple sets without waiters do not accumulate permissions`() {
        // given: an AutoResetLatch with no waiting threads
        val latch = AutoResetLatch()

        // when: multiple set calls are performed
        assertEquals(0, latch.set())
        assertEquals(0, latch.set())
        assertEquals(0, latch.set())

        // and: a thread calls await afterwards
        val result = latch.await(Duration.ofMillis(100))

        // then: previous set calls have no effect
        assertFalse(result)
    }
}
