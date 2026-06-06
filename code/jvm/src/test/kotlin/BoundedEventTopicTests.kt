import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

class UnitTestBoundedEventTopic {
    @Test
    fun `when publishing with max size, discards oldest`() {
        // give: an UnboundedEventTopic with capacity 2
        val topic = BoundedEventTopic<Int>(2)

        topic.publish(1)
        topic.publish(2)

        // when: adding another event
        topic.publish(3)

        // then: publish is successful
        assertTrue(topic.publish(4) is BoundedEventTopic.PublishResult.Success)
    }

    @Test
    fun `oldest events are discarded when capacity exceeded`() {
        // given:
        val topic = BoundedEventTopic<Int>(3)

        //
        topic.publish(1)
        topic.publish(2)
        topic.publish(3)
        topic.publish(4)
        topic.publish(5)

        val sub = topic.subscribe()!!
        val timeout = Duration.ofMillis(100)

        val results = ConcurrentLinkedQueue<Int>()

        repeat(3) {
            when (val r = sub.read(timeout)) {
                is BoundedEventTopic.Subscription.ReadResult.Success -> {
                    results.add(r.event)
                }
                else -> fail("Expected success but got $r")
            }
        }

        //
        assertEquals(listOf(3, 4, 5), results.toList())
    }

    @Test
    fun `when publishing to closed topic returns failure`() {
        // given: an UnboundedEventTopic with capacity 1
        val topic = BoundedEventTopic<Int>(2)

        // when: topic is closed
        topic.close()

        // and: trying to publish
        val pub = topic.publish(1)

        // then: publish returns Closed
        assertTrue(pub is BoundedEventTopic.PublishResult.Closed)
    }

    @Test
    fun `subscriber should timeout trying to read with no new events`() {
        // given: a BoundedEventTopic with capacity 200
        val topic = BoundedEventTopic<Int>(200)

        // and: a subscriber
        val reader = topic.subscribe()

        // and: a publisher publishes an event
        topic.publish(1)

        // when: subscriber tries to read with a timeout
        val readResult1 = reader?.read(Duration.ofMillis(100))
        val readResult2 = reader?.read(Duration.ofMillis(100))

        // then: first read returns Success
        assertTrue(readResult1 is BoundedEventTopic.Subscription.ReadResult.Success)
        // and: the second one returns Timeout (no more events left to read)
        assertTrue(readResult2 is BoundedEventTopic.Subscription.ReadResult.Timeout)
    }

    @Test
    fun `events are read in order`() {
        val topic = BoundedEventTopic<Int>(100)

        repeat(50) { topic.publish(it) }

        val sub = topic.subscribe()!!
        val timeout = Duration.ofMillis(100)

        var last = -1

        repeat(50) {
            val r = sub.read(timeout)
            if (r is BoundedEventTopic.Subscription.ReadResult.Success) {
                assertTrue(r.event > last)
                last = r.event
            }
        }
    }

    @Test
    fun `multiple subscribers read independently`() {
        val topic = BoundedEventTopic<Int>(5)

        repeat(5) { topic.publish(it) }

        val sub1 = topic.subscribe()!!
        val sub2 = topic.subscribe()!!

        val timeout = Duration.ofMillis(100)

        val r1 = mutableListOf<Int>()
        val r2 = mutableListOf<Int>()

        repeat(5) {
            r1.add((sub1.read(timeout) as BoundedEventTopic.Subscription.ReadResult.Success).event)
            r2.add((sub2.read(timeout) as BoundedEventTopic.Subscription.ReadResult.Success).event)
        }

        assertEquals(r1, r2)
    }

    @Test
    fun `slow subscriber skips overwritten events`() {
        val topic = BoundedEventTopic<Int>(3)

        val sub = topic.subscribe()!!

        repeat(10) { topic.publish(it) }

        val timeout = Duration.ofMillis(100)
        val results = mutableListOf<Int>()

        repeat(3) {
            val r = sub.read(timeout)
            if (r is BoundedEventTopic.Subscription.ReadResult.Success) {
                results.add(r.event)
            }
        }

        // só deve ver os últimos 3
        assertEquals(listOf(7, 8, 9), results)
    }
}

class StressTestBoundedEventTopic {
    @Test
    fun `test single reader doesn't go back on the read index`() {
        val nOfThreads = 6
        val nOfReps = 100
        // given: a BoundedEventTopic with capacity 600
        val topic = BoundedEventTopic<Int>(600)
        val receivedEvents = ConcurrentLinkedQueue<Int>()

        TestHelper(Thread.ofPlatform()).runTest { helper ->
            // when: multiple threads publish new events
            repeat(nOfThreads) { threadId ->
                helper.startThread {
                    repeat(nOfReps) { id ->
                        topic.publish(threadId * nOfReps + id)
                    }
                }
            }

            helper.startThread {
                val reader = topic.subscribe()!!
                var lastIdx = 0L

                // and: the subscriber reads events
                repeat(nOfReps * nOfThreads) {
                    when (val event = reader.read(Duration.ofSeconds(2))) {
                        is BoundedEventTopic.Subscription.ReadResult.Success -> {
                            // then: the read events always have bigger identifiers
                            assert(event.startIndex >= lastIdx) {
                                "Index went backwards: ${event.startIndex} <= $lastIdx"
                            }
                            lastIdx = event.startIndex
                            receivedEvents.add(event.event)
                        }
                        else -> error("Unexpected event: $event")
                    }
                }
            }
        }

        // and: the last event accounts for all published events
        assertEquals(nOfThreads * nOfReps, receivedEvents.size)
    }

    @Test
    fun `concurrent publishers and readers`() {
        val topic = BoundedEventTopic<Int>(100)
        val results = ConcurrentLinkedQueue<Int>()

        TestHelper(Thread.ofPlatform()).runTest { helper ->
            repeat(4) {
                helper.startThread {
                    repeat(100) { id ->
                        topic.publish(id)
                    }
                }
            }

            helper.startThread {
                val sub = topic.subscribe()!!
                repeat(40) {
                    val r = sub.read(Duration.ofMillis(100))
                    if (r is BoundedEventTopic.Subscription.ReadResult.Success) {
                        results.add(r.event)
                    }
                }
            }
        }

        assertTrue(results.isNotEmpty())
    }

    @Test
    fun `subscribe during publishing`() {
        val topic = BoundedEventTopic<Int>(5)

        TestHelper(Thread.ofPlatform()).runTest { helper ->
            helper.startThread {
                repeat(10) { topic.publish(it) }
            }

            helper.startThread {
                val sub = topic.subscribe()!!
                val r = sub.read(Duration.ofSeconds(2))
                assertTrue(r is BoundedEventTopic.Subscription.ReadResult.Success)
            }
        }
    }

    @Test
    fun `close wakes up waiting readers`() {
        val topic = BoundedEventTopic<Int>(5)
        val sub = topic.subscribe()!!

        TestHelper(Thread.ofPlatform()).runTest { helper ->
            helper.startThread {
                Thread.sleep(50)
                topic.close()
            }

            val r = sub.read(Duration.ofSeconds(2))
            assertTrue(r is BoundedEventTopic.Subscription.ReadResult.Closed)
        }
    }
}
