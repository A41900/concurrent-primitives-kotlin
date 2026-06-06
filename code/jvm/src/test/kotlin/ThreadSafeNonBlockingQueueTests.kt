import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals

class UTestThreadSafeNonBlockingQueue {
    @Test
    fun `next without hasNext on empty queue may throw NoSuchElementException`() {
        val queue = ThreadSafeNonBlockingQueue<Int>()
        val iterator = queue.iterator()

        assertThrows<NoSuchElementException> {
            iterator.next()
        }
    }

    @Test
    fun `next without hasNext may throw NoSuchElementException`() {
        val queue = ThreadSafeNonBlockingQueue<Int>()
        (1..3).forEach { queue.add(it) }
        val iterator = queue.iterator()

        // Exhaust the iterator
        while (iterator.hasNext()) {
            iterator.next()
        }

        // Now next() MUST throw because current is null
        assertThrows<NoSuchElementException> {
            iterator.next()
        }
    }

    @Test
    fun `next without hasNext after exhausting iterator may throw NoSuchElementException`() {
        val queue = ThreadSafeNonBlockingQueue<Int>()
        (1..3).forEach { queue.add(it) }
        val iterator = queue.iterator()

        // exhaust the iterator properly
        while (iterator.hasNext()) iterator.next()

        // now call next() without hasNext() — may throw
        assertThrows<NoSuchElementException> {
            iterator.next()
        }
    }

    @Test
    fun `iterator returns all elements in FIFO order`() {
        val queue = ThreadSafeNonBlockingQueue<Int>()
        (1..5).forEach { queue.add(it) }

        val result = queue.iterator().asSequence().toList()

        assertEquals(listOf(1, 2, 3, 4, 5), result)
    }

    @Test
    fun `iterator on empty queue has no elements`() {
        val queue = ThreadSafeNonBlockingQueue<Int>()

        assertFalse(queue.iterator().hasNext())
    }

    @Test
    fun `iterator may return removed elements (weak consistency)`() {
        val queue = ThreadSafeNonBlockingQueue<Int>()
        (1..5).forEach { queue.add(it) }

        val iterator = queue.iterator()

        // drain the queue while iterating
        repeat(5) { queue.removeOrNull() }

        // weakly consistent: no exception must be thrown, even if elements were removed
        assertDoesNotThrow {
            while (iterator.hasNext()) {
                iterator.next()
            }
        }
    }
}

class STestThreadSafeNonBlockingQueue {
    @Test
    fun testThreadSafeNonBlockingQueueAddAndRemove() {
        val nOfThreads = 16
        val nOfReps = 10_000

        val queue = ThreadSafeNonBlockingQueue<Int>()

        val threads: List<Thread> =
            List(nOfThreads) {
                Thread.ofPlatform().start {
                    repeat(nOfReps) {
                        queue.add(1)
                    }
                }
            }

        threads.forEach { t ->
            t.join()
        }

        var nodes = 0

        while (queue.removeOrNull() != null) {
            nodes++
        }

        assertEquals(nOfThreads * nOfReps, nodes)
    }

    @Test
    fun `hasNext followed by next never throws NoSuchElementException`() {
        val queue = ThreadSafeNonBlockingQueue<Int>()
        (1..100).forEach { queue.add(it) }

        val iterator = queue.iterator()

        // even with concurrent removes, hasNext -> next must never throw
        val remover =
            Thread {
                repeat(100) { queue.removeOrNull() }
            }
        remover.start()

        assertDoesNotThrow {
            while (iterator.hasNext()) {
                iterator.next()
            }
        }

        remover.join()
    }

    @Test
    fun `stress test iterator under concurrent adds and removes`() {
        val nOfThreads = 16
        val nOfReps = 10_000
        val queue = ThreadSafeNonBlockingQueue<Int>()

        val threads = mutableListOf<Thread>()

        // adder threads
        repeat(nOfThreads / 4) { threadId ->
            threads +=
                Thread.ofPlatform().start {
                    repeat(nOfReps) {
                        queue.add(threadId * nOfReps + it)
                    }
                }
        }

        // remover threads
        repeat(nOfThreads / 4) {
            threads +=
                Thread.ofPlatform().start {
                    repeat(nOfReps) {
                        queue.removeOrNull()
                    }
                }
        }

        // iterator threads — must never throw
        repeat(nOfThreads / 2) {
            threads +=
                Thread.ofPlatform().start {
                    repeat(nOfReps) {
                        assertDoesNotThrow {
                            val iterator = queue.iterator()
                            while (iterator.hasNext()) {
                                iterator.next()
                            }
                        }
                    }
                }
        }

        // wait for all threads to finish
        threads.forEach { it.join() }
    }
}
