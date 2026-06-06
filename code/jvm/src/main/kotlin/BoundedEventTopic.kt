import java.io.Closeable
import java.time.Duration
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Thread-safe bounded publish-subscribe topic.
 *
 * This implementation follows a monitor-style synchronization design,
 * protecting all mutable state with a single [ReentrantLock] and a
 * shared [Condition].
 *
 * Events are stored in a bounded circular buffer and are identified
 * by monotonically increasing global indexes.
 *
 * Each subscriber maintains an independent read cursor, allowing
 * multiple subscribers to consume the same events independently.
 *
 * When the buffer becomes full, newly published events overwrite
 * the oldest stored events.
 *
 * Subscribers that fall behind by more than [capacity] events are
 * automatically advanced to the oldest event still available.
 *
 * @param capacity maximum number of events retained by the topic.
 */

class BoundedEventTopic<T>(
    val capacity: Int,
) : Closeable {

    private val mutex = ReentrantLock()
    private val condition = mutex.newCondition()

    private val eventBuffer = Array<Any?>(capacity) { null }
    private var eventIdx: Long = 0L

    private val subscribers: MutableList<Subscription<T>> = ArrayList()
    private var closed: Boolean = false

    /**
     * Publishes [event] to the topic, making it available to all current subscribers.
     * If the buffer is full, the oldest event is overwritten.
     *
     * @return [PublishResult.Success] if published, [PublishResult.Closed] if the topic is closed.
     */
    fun publish(event: T): PublishResult {
        mutex.withLock {
            if (closed) {
                return PublishResult.Closed
            }
            saveEvent(event)
            condition.signalAll()
            return PublishResult.Success
        }
    }

    /****
     * Stores a new event in the circular buffer.
     *
     * The event is assigned the current global event index and written
     * to the corresponding buffer position. If the buffer is already
     * full, the oldest event is overwritten.
     */
    private fun saveEvent(event: T)  {
        val pos = (eventIdx % capacity).toInt()
        eventBuffer[pos] = event
        eventIdx++
    }

    /**
     * The result of a `publish` call.
     */
    sealed interface PublishResult {
        // Publish was done successfully
        data object Success : PublishResult

        // Publish cannot be done because the topic is closed
        data object Closed : PublishResult
    }

    /**
     * Creates a new subscription.
     *
     * The subscription starts reading from the oldest event currently
     * available in the topic.
     *
     * @return a new subscription, or null if the topic is closed.
     */
    fun subscribe(): Subscription<T>? {
        mutex.withLock {
            if (closed) {
                return null
            }
            val newSub = Subscriber()
            subscribers.add(newSub)
            return newSub
        }
    }

    /**
     * Closes the topic. After this call:
     * - [publish] returns [PublishResult.Closed]
     * - All blocked or future [Subscription.read] calls return [Subscription.ReadResult.Closed]
     */
    override fun close() {
        mutex.withLock {
            closed = true
            condition.signalAll()
        }
    }

    //Represents a subscription to the topic.
    interface Subscription<T> : Closeable {
        /**
         * Reads the next available event for this subscription.
         *
         * If no event is currently available, the calling thread waits
         * until:
         *
         * - a new event is published;
         * - the topic is closed;
         * - the timeout expires;
         * - the thread is interrupted.
         *
         * Subscribers that have fallen behind the retention window are
         * automatically advanced to the oldest available event.
         *
         * @param timeout maximum time to wait.
         * @return the result of the read operation.
         * @throws InterruptedException if the waiting thread is interrupted.
         */
        @Throws(InterruptedException::class)
        fun read(timeout: Duration): ReadResult<T>

        // Represents the result of a read on a subscription.
        sealed interface ReadResult<out T> {
            //Read cannot be done because topic is closed.
            data object Closed : ReadResult<Nothing>

            //Read was not done because the timeout was exceeded.
            data object Timeout : ReadResult<Nothing>

            // Read was done successfully and the event was returned.
            data class Success<T>(
                // The read event
                val event: T,
                // Index for the read event
                val startIndex: Long,
            ) : ReadResult<T>
        }
    }


    /**
     * [Subscription] implementation. Tracks [startIdx] — the global event index
     * of the next event this subscription should read.
     *
     * Initialized to [eventIdx] - [size] so that a new subscriber starts from the oldest
     * event currently available in the buffer.
     */
    inner class Subscriber : Subscription<T> {
        private val oldestIdx get() = maxOf(0L, eventIdx - capacity.toLong())
        private var nextIdx = oldestIdx

        override fun read(timeout: Duration): Subscription.ReadResult<T> {

            mutex.withLock {
                // Set timeout
                var remainingNanos = timeout.toNanos()
                if (remainingNanos < 0) {
                    return Subscription.ReadResult.Timeout
                }

                while (true) {
                    if (closed) {
                        return Subscription.ReadResult.Closed
                    }

                    // If subscriber is behind, adjust next event to read to current oldest
                    if (nextIdx < oldestIdx) {
                        nextIdx = oldestIdx
                    }

                    if (nextIdx < eventIdx) {

                        val readIdx = (nextIdx % capacity).toInt()

                        @Suppress("UNCHECKED_CAST")
                        val event = eventBuffer[readIdx] as T
                        val curr = nextIdx
                        nextIdx++

                        return Subscription.ReadResult.Success(event, curr)
                    }

                    // Check timeout
                    if (remainingNanos < 0) {
                        return Subscription.ReadResult.Timeout
                    }

                    try {
                        if (nextIdx == eventIdx) {
                            // Await new events and restart timer with remaining time
                            remainingNanos = condition.awaitNanos(remainingNanos)
                        }
                    } catch (e: InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw e
                    }
                }
            }
        }

        override fun close() {
            mutex.withLock {
                subscribers.remove(this)
            }
        }
    }
}
