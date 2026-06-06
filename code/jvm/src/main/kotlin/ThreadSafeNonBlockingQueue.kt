import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Unbounded First-In-First-Out (FIFO) queue with thread-safe [add] and [removeOrNull] operations
 * and a _weakly consistent_ iterator.
 *
 * Operations:
 * - [add] - adds an element to the end of the queue.
 * - [removeOrNull] - removes and returns the element at the front of the queue, or `null` if the queue is empty.
 * - [iterator] - returns a weakly_consistent iterator that enables traversal of the queue from front to end,
 * may return elements that were already removed
 *
 * Thread safety is achieved via a [java.util.concurrent.locks.ReentrantLock] that guards shared state modifications.
 * The iterator does not hold the lock during traversal, providing weakly consistent semantics —
 * it reflects the state of the queue at some point during iteration and will never throw
 * [ConcurrentModificationException].
 *
 * Implemented using a linked list structure, where each node contains a value and a reference to the next node.
 */
class ThreadSafeNonBlockingQueue<T> : Iterable<T> {
    /**
     * Node of the queue, containing a value and a reference to the next node.
     */
    class Node<T>(val value: T, var next: Node<T>? = null)

    private var head: Node<T>? = null
    private var tail: Node<T>? = null

    private val mutex = ReentrantLock()

    /**
     * Adds an element to the _end_ of the queue.
     */
    fun add(value: T) {
        mutex.withLock {
            val newNode = Node(value)

            if (tail == null) {
                // If queue is empty, set both head and tail to newNode
                head = newNode
                tail = newNode
            } else {
                // Otherwise, add newNode to the end and update tail
                tail?.next = newNode
                tail = newNode
            }
        }
    }

    /**
     * Removes and returns the element in the front of the queue, or returns `null` if the queue is empty.
     */
    fun removeOrNull(): T? {
        mutex.withLock {
            // If list is empty no items to remove
            if (head == null) {
                return null
            }

            // Get the value for the first node in queue and move head to the next node to remove it
            val value = head?.value
            head = head?.next

            // If the queue is now empty, set tail to null as well
            if (head == null) {
                tail = null
            }
            return value
        }
    }

    /**
     * Returns an iterator to the queue.
     */
    override fun iterator(): WeaklyConsistentIterator<T> {
        mutex.withLock {
            return WeaklyConsistentIterator(head)
        }
    }

    /**
     * Iterator to the queue, starting from the front and moving to the end, providing a _weakly consistent_ behavior,
     * as described in
     * [https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/package-summary.html#Weakly](Concurrent Collections):
     * - Iteration MUST not block other operations in the queue.
     * - A call to `next` must succeed if it was proceeded by a call to `hasNext`, which returned `true`.
     * - MAY return an element that was already removed when the `next` method is called.
     * That is, use of the iterator in a `for` statement will never result in an exception.
     * It is assumed that an iterator instance will not be called concurrently from multiple-threads.
     */
    class WeaklyConsistentIterator<T>(head: Node<T>?) : Iterator<T> {
        private var current: Node<T>? = head

        /**
         * Returns `true` if there are still more elements to iterate, `false` otherwise.
         */
        override fun hasNext(): Boolean {
            return current != null
        }

        /**
         * Returns the next element on the iteration.
         * - MUST return an element if the call was preceded with an `hasNext` call that returned `true`.
         * - MAY throw `NoSuchElementException` when not preceded with an `hasNext` call that returned `true`.
         * - MUST not throw `ConcurrentModificationException`.
         */
        override fun next(): T {
            val node = current ?: throw NoSuchElementException()
            current = node.next
            return node.value
        }
    }
}
