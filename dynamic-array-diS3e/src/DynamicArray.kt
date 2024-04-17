package mpp.dynamicarray

import kotlinx.atomicfu.*

interface DynamicArray<E> {
    /**
     * Returns the element located in the cell [index],
     * or throws [IllegalArgumentException] if [index]
     * exceeds the [size] of this array.
     */
    fun get(index: Int): E

    /**
     * Puts the specified [element] into the cell [index],
     * or throws [IllegalArgumentException] if [index]
     * exceeds the [size] of this array.
     */
    fun put(index: Int, element: E)

    /**
     * Adds the specified [element] to this array
     * increasing its [size].
     */
    fun pushBack(element: E)

    /**
     * Returns the current size of this array,
     * it increases with [pushBack] invocations.
     */
    val size: Int
}

class DynamicArrayImpl<E> : DynamicArray<E> {
    private val head = atomic(Core(INITIAL_CAPACITY))

    private val helpStatus = atomic(NEED_NO_HELP)

    private fun tryHelp() {
        if (helpStatus.compareAndSet(NEED_HELP, IS_HELPING)) {
            help()
        }
    }

    private fun needHelp() {
        if (!helpStatus.compareAndSet(NEED_NO_HELP, NEED_HELP)) {
            helpStatus.compareAndSet(IS_HELPING, NEED_HELP_AFTERWARDS)
        }
    }

    private fun help() {
        val head = this.head.value
        for (index in 0 until head.array.size) {
            var oldElement: Any?
            while (true) {
                oldElement = head.array[index].value
                if (head.array[index].compareAndSet(oldElement, Frozen(oldElement))) break
            }
            transferFrozen(oldElement, head, index)
        }

        val next = head.next.value!!
        this.head.getAndSet(next)

        if (!helpStatus.compareAndSet(IS_HELPING, NEED_NO_HELP)) {
            helpStatus.compareAndSet(NEED_HELP_AFTERWARDS, NEED_HELP)
        }
    }

    private fun transferFrozen(frozenValue: Any?, core: Core, index: Int) {
        val nextCore = core.next.value!!
        val nextElement = nextCore.array[index].value
        if (nextElement == FREE) nextCore.array[index].compareAndSet(FREE, frozenValue)
        core.array[index].getAndSet(MOVED)
    }

    private fun findLastCore(startCore: Core): Core {
        var core = startCore
        while (core.next.value != null) {
            core = core.next.value!!
        }
        return core
    }

    private fun nextCapacity(oldCapacity: Int) = 2 * oldCapacity

    private fun findAppropriateCore(index: Int): Core {
        var core = this.head.value
        while (index >= core.array.size) {
            core = core.next.value!!
        }
        return core
    }

    override fun get(index: Int): E {
        require(index < size) { "Index must be < size" }

        var core = findAppropriateCore(index)
        while (true) {
            val element = core.array[index].value
            if (element != MOVED) return if (element is Frozen) element.element as E else element as E
            core = core.next.value!!
        }
    }

    override fun put(index: Int, element: E) {
        require(index < size) { "Index must be < size" }

        var core: Core = findAppropriateCore(index)
        while (true) {
            val oldElement = core.array[index].value
            if (oldElement is Frozen) transferFrozen(oldElement.element, core, index)
            else if (oldElement == MOVED) core = core.next.value!!
            else if (core.array[index].compareAndSet(oldElement, element)) return
        }
    }

    override fun pushBack(element: E) {
        var core = this.head.value
        while (true) {
            core = findLastCore(core)
            val index = size
            if (index == core.array.size) {
                val newCore = Core(nextCapacity(index))
                newCore.array[index].getAndSet(element)
                if (core.next.compareAndSet(null, newCore)) {
                    asize.incrementAndGet()
                    needHelp()
                    return
                }
            } else if (index < core.array.size && core.array[index].compareAndSet(FREE, element)) {
                asize.incrementAndGet()
                tryHelp()
                return
            }
        }
    }

    private val asize = atomic(0)
    override val size: Int get() = asize.value
}

private class Frozen(val element: Any?)

private class Core(capacity: Int) {
    val array = atomicArrayOfNulls<Any>(capacity)
    val next = atomic<Core?>(null)

    init {
        for (i in 0 until capacity) array[i].getAndSet(FREE)
    }
}

private const val NEED_NO_HELP = 0
private const val NEED_HELP = 1
private const val IS_HELPING = 2
private const val NEED_HELP_AFTERWARDS = 3

private val MOVED: Any = Any()
private val FREE: Any = Any()

private const val INITIAL_CAPACITY = 1 // DO NOT CHANGE ME