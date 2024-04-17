import java.util.concurrent.*
import java.util.concurrent.atomic.*

class FlatCombiningQueue<E> : Queue<E> {
    private val queue = ArrayDeque<E>() // sequential queue
    private val combinerLock = AtomicBoolean(false) // unlocked initially
    private val tasksForCombiner = AtomicReferenceArray<Any?>(TASKS_FOR_COMBINER_SIZE)
    private fun combinerHelp() {
        for (taskIndex in 0 until TASKS_FOR_COMBINER_SIZE) {
            val task = tasksForCombiner.get(taskIndex)
            if (task is Dequeue) {
                val head = queue.removeFirstOrNull()
                tasksForCombiner.set(taskIndex, Result(head))
            } else if (task != null && task !is Result<*>) {
                queue.addLast(task as E)
                tasksForCombiner.set(taskIndex, Result(null))
            }
        }
        combinerLock.set(false)
    }

    override fun enqueue(element: E) {
        var index = randomCellIndex()
        while (!tasksForCombiner.compareAndSet(index, null, element)) {
            index = randomCellIndex()
        }
        while (true) {
            if (combinerLock.compareAndSet(false, true)) {
                combinerHelp()
            }
            if (tasksForCombiner.get(index) is Result<*>) {
                tasksForCombiner.set(index, null)
                return
            }
        }
    }

    override fun dequeue(): E? {
        var index = randomCellIndex()
        while (!tasksForCombiner.compareAndSet(index, null, Dequeue)) {
            index = randomCellIndex()
        }
        while (true) {
            if (combinerLock.compareAndSet(false, true)) {
                combinerHelp()
            }
            if (tasksForCombiner.get(index) is Result<*>) {
                return (tasksForCombiner.getAndSet(index, null) as Result<*>).value as E?
            }
        }
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(tasksForCombiner.length())
}

private const val TASKS_FOR_COMBINER_SIZE = 3 // Do not change this constant!

// TODO: Put this token in `tasksForCombiner` for dequeue().
// TODO: enqueue()-s should put the inserting element.
private object Dequeue

// TODO: Put the result wrapped with `Result` when the operation in `tasksForCombiner` is processed.
private class Result<V>(
    val value: V
)