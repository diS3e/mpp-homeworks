package dijkstra

import kotlinx.atomicfu.atomic
import java.util.*
import java.util.concurrent.Phaser
import kotlin.Comparator
import kotlin.concurrent.thread

private val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> Integer.compare(o1!!.distance, o2!!.distance) }

// Returns `Integer.MAX_VALUE` if a path has not been found.
fun shortestPathParallel(start: Node) {
    val workers = Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    start.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = Collections.nCopies(workers, PriorityQueue(NODE_DISTANCE_COMPARATOR))
    val random = Random(0)
    val size = Size()

    val q1 = q[random.nextInt(workers)]
    synchronized(q1) {
        q1.add(start)
    }

    size.size.incrementAndGet()
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    repeat(workers) {
        thread {
            while (true) {
                val cur: Node? = synchronized(q) {
                    val (i1, i2) = random.nextInt(workers) to random.nextInt(workers)
                    val (q1, q2) = q[i1] to q[i2]
                    synchronized(q1) {
                        synchronized(q2) {
                            val (v1, v2) = q1.peek() to q2.peek()
                            if (v1 == null) v2 else if (v2 == null) v1 else
                                if (v1.distance < v2.distance) q1.poll() else q2.poll()
                        }
                    }
                }
                if (cur == null) {
                    if (size.size.compareAndSet(0, 0)) {
                        break
                    } else {
                        continue
                    }
                }
                for (e in cur.outgoingEdges) {
                    while (true) {
                        val prev = e.to.distance
                        val next = cur.distance + e.weight
                        if (prev > next) {
                            if (e.to.casDistance(prev, next)) {
                                val top = q[random.nextInt(workers)]
                                synchronized(top) {
                                    top.add(e.to)
                                }
                                size.size.incrementAndGet()
                            } else {
                                continue
                            }
                        }
                        break
                    }
                }
                size.size.decrementAndGet()
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
}

class Size {
    val size = atomic(0)
}