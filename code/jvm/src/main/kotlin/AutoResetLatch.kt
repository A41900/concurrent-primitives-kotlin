import java.time.Duration
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**

 * Monitor-style implementation of an auto-reset latch.
 *
 * Threads may wait for a signal by invoking [await]. A call to [set]
 * releases all threads that are waiting at that exact moment and
 * returns the number of released threads.
 *
 * The synchronizer is "auto-reset" because a call to [set] does not
 * create any permit for future calls to [await]. Only threads already
 * waiting when [set] is invoked may complete successfully.
 *
 * Synchronization is implemented using a single monitor
 * ([ReentrantLock] and [java.util.concurrent.locks.Condition]).
 *
 * State Model:
 *
 * * [activeAwaits] represents the number of threads currently waiting
 * in [await].
 *
 * * [setCounter] identifies the current generation of successful
 * signals. Each successful call to [set] increments this counter.
 * Waiting threads capture the current generation and complete
 * successfully when they observe a generation change.
 *
 * Invariant:
 *
 * * activeAwaits >= 0
 * * setCounter >= 0
 */

class AutoResetLatch {

    private val mutex = ReentrantLock()
    private val awaitingCondition = mutex.newCondition()

    private var activeAwaits = 0
    private var setCounter = 0

    /**

     * Signals the latch.
     *
     * All threads waiting at the instant of this invocation become eligible
     * to complete successfully. Future calls to [await] are not affected by
     * this signal.
     *
     * @return the number of threads released by this invocation.
     */

    fun set(): Long =
        mutex.withLock {
            val n = activeAwaits
            activeAwaits = 0

            if (n > 0) {
                setCounter++
                awaitingCondition.signalAll()
            }

            n.toLong()
        }

    /**

     * Waits until a call to [set] occurs.
     *
     * The calling thread blocks while no new signal generation is observed.
     *
     * The wait may terminate:
     *
     * * successfully, if a call to [set] occurs;
     * * unsuccessfully, if the timeout expires;
     * * by throwing [InterruptedException] if the thread is interrupted
     * before a signal is observed.
     *
     * If an interruption races with a successful [set], the signal wins and
     * the method returns `true`, preserving the guarantee that every thread
     * released by a call to [set] completes successfully.
     *
     * @param timeout maximum waiting time.
     * @return `true` if a signal was observed, `false` if the timeout expired.
     * @throws InterruptedException if interrupted before a signal is observed.
     */

    @Throws(InterruptedException::class)
    fun await(timeout: Duration): Boolean {
        mutex.withLock {
            var remainingNanos = timeout.toNanos()
            if (remainingNanos <= 0) return false

            activeAwaits++
            val mySet = setCounter

            try {
                while (true) {
                    if (mySet != setCounter) {
                        return true
                    }

                    if (remainingNanos <= 0) {
                        activeAwaits--
                        return false
                    }

                    remainingNanos = awaitingCondition.awaitNanos(remainingNanos)
                }
            } catch (e: InterruptedException) {
                if (mySet != setCounter) {
                    activeAwaits--
                    Thread.currentThread().interrupt()
                    return true
                }
                throw e
            }
        }
    }
}
