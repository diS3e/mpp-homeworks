import java.util.concurrent.atomic.*

/**
 * @author Самоделов Евгений
 */
class FAABasedQueue<E> : Queue<E> {
    private val enqIdx = AtomicLong(0)
    private val deqIdx = AtomicLong(0)
    private val head: AtomicReference<Segment>
    private val tail: AtomicReference<Segment>

    init {
       val segment = Segment(0)
        head = AtomicReference(segment)
        tail = AtomicReference(segment)
    }

    private fun findSegment(segment: Segment, segmentId: Long) : Segment {
        var curSegment: Segment = segment
        while (curSegment.id != segmentId) {
            if (curSegment.next.get() == null) {
                if (!curSegment.next.compareAndSet(null, Segment(curSegment.id + 1))) {
                    continue
                }
            }
            curSegment = curSegment.next.get() as Segment
        }
        return curSegment
    }

    private fun moveTailForward(segment: Segment)  {
        if (segment.id != tail.get().id) {
            tail.set(segment)
        }
    }

    private fun moveHeadForward(segment: Segment)  {
        if (segment.id != head.get().id) {
            head.set(segment)
        }
    }

    override fun enqueue(element: E) {
        while (true) {
            val curTail = tail.get()
            val i = enqIdx.getAndIncrement()
            val s = findSegment(curTail, i / SEGMENT_SIZE)
            moveTailForward(s)
            if (s.cells.compareAndSet(i.toInt() % SEGMENT_SIZE, null, element)) return
        }
    }

    override fun dequeue(): E? {
        while (true) {
            if (deqIdx.get() >= enqIdx.get()) return null
            val curHead = head.get()
            val i = deqIdx.getAndIncrement()
            val s = findSegment(curHead, i / SEGMENT_SIZE)
            moveHeadForward(s)
            if (s.cells.compareAndSet(i.toInt() % SEGMENT_SIZE, null, Any())) continue
            return s.cells.getAndSet(i.toInt() % SEGMENT_SIZE, null) as E?
        }
    }
}

private class Segment(val id: Long) {
    val next = AtomicReference<Segment?>(null)
    val cells = AtomicReferenceArray<Any?>(SEGMENT_SIZE)
}

// DO NOT CHANGE THIS CONSTANT
private const val SEGMENT_SIZE = 2
