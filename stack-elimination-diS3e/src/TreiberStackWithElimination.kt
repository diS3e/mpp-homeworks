import java.util.concurrent.*
import java.util.concurrent.atomic.*

/**
 * @author Самоделов Евгений
 */
open class TreiberStackWithElimination<E> : Stack<E> {
    private val stack = TreiberStack<E>()

    private val eliminationArray = AtomicReferenceArray<E?>(ELIMINATION_ARRAY_SIZE)

    override fun push(element: E) {
        if (tryPushElimination(element)) return
        stack.push(element)
    }

    protected open fun tryPushElimination(element: E): Boolean {
        val index = randomCellIndex()
        if (eliminationArray.compareAndSet(index, null, element)) {
            repeat(ELIMINATION_WAIT_CYCLES) {}
            if (!eliminationArray.compareAndSet(index, element, null)) {
                return true
            }
        }

        return false
    }

    override fun pop(): E? = tryPopElimination() ?: stack.pop()

    private fun tryPopElimination(): E? {
        return eliminationArray.getAndSet(randomCellIndex(), null)
    }

    private fun randomCellIndex(): Int =
        ThreadLocalRandom.current().nextInt(eliminationArray.length())

    companion object {
        private const val ELIMINATION_ARRAY_SIZE = 2 // Do not change!
        private const val ELIMINATION_WAIT_CYCLES = 1 // Do not change!

        // Initially, all cells are in EMPTY state.
        private val CELL_STATE_EMPTY = null

        // `tryPopElimination()` moves the cell state
        // to `RETRIEVED` if the cell contains element.
        private val CELL_STATE_RETRIEVED = Any()
    }
}
