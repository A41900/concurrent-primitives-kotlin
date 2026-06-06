import java.io.Closeable
import java.time.Duration
import java.util.LinkedList
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * A structured concurrency scope that manages a group of threads and child scopes,
 * enforcing a maximum number of concurrently alive threads ([maxAliveThreads]).
 *
 * Threads submitted beyond [maxAliveThreads] are queued and started automatically
 * as running threads complete. This provides bounded parallelism without rejecting work.
 *
 * A scope can be in one of three states:
 * - [State.OPEN]: accepts the creation of new threads and child scopes
 * - [State.CLOSING]: no longer accepts creation, waiting for existing threads and children to finish
 * - [State.CLOSED]: all threads and child scopes have completed
 *
 * When a child scope completes, it notifies its parent via [childCompleted], allowing the
 * parent to transition to check if there has to be a transition of state to [State.CLOSED].
 *
 * IF [join] is called without a previous call to [close] or [cancel] it will always exit
 * from timeout.
 *
 * @param name name of the threadScope
 * @param threadBuilder a builder function for creating new threads
 * @param maxAliveThreads the maximum number of threads that can be alive concurrently in this scope
 * @param parent the parent scope, or null if this is a root scope
 *
 * @throws InterruptedException if the thread is interrupted while waiting in [join]
 */
class BoundedThreadScope(
    name: String,
    private val threadBuilder: Thread.Builder,
    private val maxAliveThreads: Int,
    private val parent: BoundedThreadScope? = null,
) : Closeable {
    private val mutex = ReentrantLock()
    private val condition = mutex.newCondition()

    private enum class State {
        OPEN,
        CLOSING,
        CLOSED,
    }

    private var state = State.OPEN
    private val awaitingThreads = LinkedList<Thread>()
    private val activeThreads = LinkedList<Thread>()
    private val childScope = LinkedList<BoundedThreadScope>()

    // Creates and conditionally starts a new thread in the scope, if the scope is not closed
    fun newThread(runnable: Runnable): Thread? {
        mutex.withLock {
            if (state != State.OPEN) return null
            val thread =
                threadBuilder.unstarted({
                    try {
                        runnable.run()

                        // When the thread finished the work, it is responsible for starting the next thread in the queue, if possible
                    } finally {
                        startNextThread()
                    }
                })
            if (activeThreads.size < maxAliveThreads) {
                activeThreads.add(thread)
                thread.start()
            } else {
                awaitingThreads.addLast(thread)
            }
            return thread
        }
    }

    private fun startNextThread() {
        val parentToNotify =
            mutex.withLock {
                activeThreads.remove(Thread.currentThread())

                if (awaitingThreads.isNotEmpty() && activeThreads.size < maxAliveThreads) {
                    val next = awaitingThreads.removeFirst()
                    activeThreads.add(next)
                    next.start()
                }
                tryComplete()
            }
        parentToNotify?.childCompleted(this)
    }

    private fun childCompleted(child: BoundedThreadScope) {
        val parentToNotify: BoundedThreadScope? =
            mutex.withLock {
                childScope.remove(child)
                tryComplete()
            }
        parentToNotify?.childCompleted(this)
    }

    // Creates a new child scope, if the current scope is not closed
    fun newChildScope(name: String): BoundedThreadScope? {
        mutex.withLock {
            if (state != State.OPEN) return null
            val newChildScope = BoundedThreadScope(name, threadBuilder, maxAliveThreads, this)
            childScope.add(newChildScope)
            return newChildScope
        }
    }

    // Closes the current scope, disallowing the creation of any further thread  or child scope
    override fun close() {
        val parentToNotify =
            mutex.withLock {
                if (state == State.OPEN) {
                    state = State.CLOSING
                    tryComplete()
                } else {
                    null
                }
            }
        parentToNotify?.childCompleted(this)
    }

    // Waits until all threads and child scopes have completed
    @Throws(InterruptedException::class)
    fun join(timeout: Duration): Boolean {
        mutex.withLock {
            if (state == State.CLOSED) {
                return true
            }

            var remainingNanos = timeout.toNanos()
            if (remainingNanos <= 0) {
                return false
            }

            while (true) {
                try {
                    remainingNanos = condition.awaitNanos(remainingNanos)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    if (state == State.CLOSED) {
                        return true
                    }
                    throw e
                }

                if (state == State.CLOSED) {
                    return true
                }

                if (remainingNanos <= 0) {
                    return false
                }
            }
        }
    }

    // Interrupts all threads in the scope and cancels all child scopes.
    fun cancel() {
        val parentToNotify: BoundedThreadScope?
        val childrenToCancel: List<BoundedThreadScope>

        mutex.withLock {
            awaitingThreads.clear()
            state = State.CLOSING

            activeThreads.forEach { it.interrupt() }

            childrenToCancel = childScope.toList()
            parentToNotify = tryComplete()
        }

        childrenToCancel.forEach { it.cancel() }
        parentToNotify?.childCompleted(this)
    }

    /**
     * Returns true if the scope has no remaining work.
     *
     * A scope is considered completed when:
     * - there are no active threads;
     * - there are no queued threads waiting to start;
     * - there are no active child scopes.
     */
    private fun hasCompleted(): Boolean =
        activeThreads.isEmpty() && awaitingThreads.isEmpty() && childScope.isEmpty()

    /**
     * Attempts to transition the scope from CLOSING to CLOSED.
     *
     * If the scope has no remaining work, its state is updated to CLOSED,
     * all threads waiting in join() are notified, and the parent scope
     * is returned so it can be notified of this scope completion.
     *
     * Returns:
     * - the parent scope if a transition to CLOSED occurred;
     * - null otherwise.
     */
    private fun tryComplete(): BoundedThreadScope? {
        if (state == State.CLOSING && hasCompleted()) {
            state = State.CLOSED
            condition.signalAll()
            return parent
        }
        return null
    }
}
