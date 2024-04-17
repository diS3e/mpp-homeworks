import kotlinx.atomicfu.AtomicIntArray
import kotlinx.atomicfu.atomic
import kotlin.math.log2
import kotlin.math.min
/**
 * Int-to-Int hash map with open addressing and linear probes.
 */
class IntIntHashMap {
    private val core = atomic(Core(INITIAL_CAPACITY))

    /**
     * Returns value for the corresponding key or zero if this key is not present.
     *
     * @param key a positive key.
     * @return value for the corresponding or zero if this key is not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    operator fun get(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }

        core.value.tryHelp()
        return toValue(core.value.getInternal(key))
    }

    /**
     * Changes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key   a positive key.
     * @param value a positive value.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key or value are not positive, or value is equal to
     * [Integer.MAX_VALUE] which is reserved.
     */
    fun put(key: Int, value: Int): Int {
        require(key > 0) { "Key must be positive: $key" }
        require(isValue(value)) { "Invalid value: $value" }

        core.value.tryHelp()
        return toValue(core.value.putInternal(key, value))
    }

    /**
     * Removes value for the corresponding key and returns old value or zero if key was not present.
     *
     * @param key a positive key.
     * @return old value or zero if this key was not present.
     * @throws IllegalArgumentException if key is not positive.
     */
    fun remove(key: Int): Int {
        require(key > 0) { "Key must be positive: $key" }

        core.value.tryHelp()
        return toValue(core.value.putInternal(key, DEL_VALUE))
    }

    private inner class Core(val capacity: Int) {
        // Pairs of <key, value> here, the actual
        // size of the map is twice as big.
        private val map = AtomicIntArray(2 * capacity)
        private val shift: Int

        private val next = atomic<Core?>(null)

        private val moveIndex = atomic<Int?>(0)
        private val step = log2(capacity.toDouble()).toInt()
        private val completedWorkers = atomic(0)

        init {
            val mask = capacity - 1
            assert(mask > 0 && mask and capacity == 0) { "Capacity must be power of 2: $capacity" }
            shift = 32 - Integer.bitCount(mask)
        }

        fun tryHelp() {
            while (true) {
                val nextCore = next.value ?: return
                val startIndex = moveIndex.value ?: return
                if (startIndex == capacity) {
                    moveIndex.getAndSet(null)
                    return
                }
                val endIndex = min(startIndex + step, capacity)
                if (!moveIndex.compareAndSet(startIndex, endIndex)) continue
                loop@ for (index in startIndex until endIndex) {
                    val keyIndex = 2 * index
                    val valueIndex = keyIndex + 1

                    var oldValue: Int
                    while (true) {
                        oldValue = map[valueIndex].value
                        if (!isValue(oldValue)) {
                            if (map[valueIndex].compareAndSet(oldValue, MOVED_VALUE)) continue@loop
                        } else {
                            if (map[valueIndex].compareAndSet(oldValue, freeze(oldValue))) break
                        }
                    }
                    val res = nextCore.putInternal(map[keyIndex].value, freeze(oldValue))
                    assert(res == MAGIC) { "Unexpected result during tryHelp: $res" }
                    map[valueIndex].getAndSet(MOVED_VALUE)
                }
                if (completedWorkers.incrementAndGet() * step >= capacity) core.getAndSet(nextCore)
                return
            }
        }

        fun getInternal(key: Int): Int {
            var index = index(key)
            var probes = 0
            var mapKey = map[index].value
            while (mapKey != key) { // optimize for successful lookup
                if (mapKey == NULL_KEY) return NULL_VALUE // not found -- no value
                if (++probes >= MAX_PROBES) {
                    return if (next.value == null) NULL_VALUE else next.value!!.getInternal(key)
                }
                if (index == 0) index = map.size
                index -= 2
                mapKey = map[index].value
            }
            val value = map[index + 1].value
            if (isMoved(value)) return next.value!!.getInternal(key)
            return value
        }

        fun putInternal(key: Int, value: Int): Int {
            val isValueFrozen = isFrozen(value)
            var index = index(key)
            var probes = 0
            var mapKey = map[index].value
            while (mapKey != key) { // optimize for successful lookup
                if (mapKey == NULL_KEY) {
                    // not found -- claim this slot
                    if (value == DEL_VALUE) return NULL_VALUE // remove of missing item, no need to claim slot
                    if (map[index].compareAndSet(NULL_KEY, key) || map[index].value == key) break
                    else return putInternal(key, value)
                }
                // rehashing or look next
                if (++probes >= MAX_PROBES) return rehash(key, value)
                if (index == 0) index = map.size
                index -= 2
                mapKey = map[index].value
            }

            if (isValueFrozen) {
                map[index + 1].compareAndSet(NULL_VALUE, unfreeze(value))
                return MAGIC
            }

            while (true) {
                val oldValue = map[index + 1].value
                if (isMoved(oldValue)) return next.value!!.putInternal(key, value)
                if (isFrozen(oldValue)) {
                    next.value!!.putInternal(key, oldValue)
                    map[index + 1].getAndSet(MOVED_VALUE)
                    continue
                }
                if (map[index + 1].compareAndSet(oldValue, value)) return oldValue
            }
        }

        private fun rehash(key: Int, value: Int): Int {
            val nextCore = next.value
            if (nextCore != null) return nextCore.putInternal(key, value)
            val newCore = Core(nextCapacity(capacity))
            val res = newCore.putInternal(key, value)
            return if (next.compareAndSet(null, newCore)) res
            else next.value!!.putInternal(key, value)
        }

        /**
         * Returns an initial index in map to look for a given key.
         */
        fun index(key: Int): Int = (key * MAGIC ushr shift) * 2
    }
}

private const val MAGIC = -0x61c88647 // golden ratio
private const val INITIAL_CAPACITY = 2 // !!! DO NOT CHANGE INITIAL CAPACITY !!!
private const val MAX_PROBES = 8 // max number of probes to find an item
private const val NULL_KEY = 0 // missing key (initial value)
private const val NULL_VALUE = 0 // missing value (initial value)
private const val DEL_VALUE = Int.MAX_VALUE // mark for removed value
private const val FROZEN_BIT = 1 shl 31
private const val MOVED_VALUE = Int.MIN_VALUE

private fun isMoved(value: Int): Boolean = value == MOVED_VALUE

private fun isFrozen(value: Int): Boolean = value and FROZEN_BIT != 0

private fun freeze(value: Int) = value or FROZEN_BIT
private fun unfreeze(value: Int) = value and FROZEN_BIT.inv()

// Checks is the value is in the range of allowed values
private fun isValue(value: Int): Boolean = value in (1 until DEL_VALUE)

// Converts internal value to the public results of the methods
private fun toValue(value: Int): Int {
    val unfrozen = unfreeze(value)
    return if (isValue(unfrozen)) unfrozen else 0
}

private fun nextCapacity(oldCapacity: Int): Int = 2 * oldCapacity