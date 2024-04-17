import kotlinx.atomicfu.*

/**
 * @author Самоделов Евгений
 */
class AtomicArray<E>(size: Int, initialValue: E) {
    private val a = arrayOfNulls<Ref<E>>(size)

    init {
        for (i in 0 until size) a[i] = Ref(initialValue)
    }

    fun get(index: Int) = a[index]!!.value

    fun set(index: Int, value: E) {
        a[index]?.value = value
    }

    fun cas(index: Int, expected: E, update: E) = a[index]?.cas(expected, update) ?: false

    private fun getTriple(t1: Triple<Int, E, E>, t2: Triple<Int, E, E>): Triple<Int, E, CAS2Descriptor<E>> {
        val (index1, expected1, update1) = t1
        val (index2, expected2, update2) = t2
        return Triple(
            index2,
            expected2,
            CAS2Descriptor(Triple(a[index2]!!, expected2, update2), Triple(a[index1]!!, expected1, update1))
        )
    }


    fun cas2(
        index1: Int, expected1: E, update1: E,
        index2: Int, expected2: E, update2: E
    ): Boolean {
        val t1 = Triple(index1, expected1, update1)
        val t2 = Triple(index2, expected2, update2)
        var result = false
        if (index1 == index2) {
            if (expected1 == expected2) {
                result = cas(index1, expected1, update2)
            }
        } else {
            val (index, expected, descriptor) = if (index1 > index2) getTriple(t1, t2) else getTriple(t2, t1)
            if (a[index]!!.cas(expected, descriptor)) {
                descriptor.complete()
                result = descriptor.status.value == 1
            }
        }
        return result
    }
}

abstract class Descriptor {
    abstract fun complete()
    fun update(value: Int, expected: Any?, update: Any?): Any? = if (value == 1) expected else update

}


class Ref<T>(initial: T) {
    val v = atomic<Any?>(initial)

    var value: T
        set(upd) {
            v.loop { cur ->
                when (cur) {
                    is Descriptor -> cur.complete()
                    else -> if (v.compareAndSet(cur, upd))
                        return
                }
            }
        }
        get() {
            v.loop { cur ->
                when (cur) {
                    is Descriptor -> cur.complete()
                    else -> return cur as T
                }
            }
        }

    fun cas(expected: Any?, update: Any?): Boolean {
        v.loop { cur ->
            when (cur) {
                is Descriptor -> cur.complete()
                expected -> if (v.compareAndSet(cur, update)) return true
                else -> return false
            }
        }
    }
}

//class DCSSDescriptor<A>(
//    val a: Ref<A>, val expectA: A, val updateA: Any?,
//    val otherDescriptor: CAS2Descriptor<A>
//) : Descriptor() {
//    override fun complete() {
//
//    }
//}

class RDCSSDescriptor<A>(private val t: Triple<Ref<A>, A, Any?>, private val descriptor: CAS2Descriptor<A>) :
    Descriptor() {
    val status = atomic(0)

    override fun complete() {
        val status1 = if (descriptor.status.value != 0) -1 else 1
        status.compareAndSet(0, status1)
        if (status.value == 1) {
            t.first.v.compareAndSet(this, t.third)
        } else {
            t.first.v.compareAndSet(this, t.second)
        }
    }
}


class CAS2Descriptor<E>(val tA: Triple<Ref<E>, E, E>, val tB: Triple<Ref<E>, E, E>) : Descriptor() {
    val status = atomic(0)
    override fun complete() {
        val value = tB.first.v.value
        val descriptor = RDCSSDescriptor(Triple(tB.first, tB.second, this), this)
        var state = false
        if (tB.first.v.value?.equals(this) == true) {
            state = true
        } else if (tB.first.cas(tB.second, descriptor)) {
            descriptor.complete()
            state = descriptor.status.value == 1
        }
        if (state) {
            status.compareAndSet(0, 1)
        } else if (value != this) {
            status.compareAndSet(0, -1)
        } else {
            status.compareAndSet(0, 1)
        }
        tA.first.v.compareAndSet(this, update(-status.value, tA.second, tA.third))
        tB.first.v.compareAndSet(this, update(-status.value, tB.second, tB.third))
    }
}
