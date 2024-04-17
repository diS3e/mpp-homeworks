/**
 * В теле класса решения разрешено использовать только переменные делегированные в класс RegularInt.
 * Нельзя volatile, нельзя другие типы, нельзя блокировки, нельзя лазить в глобальные переменные.
 *
 * @author : Самоделов Евгений
 */
class Solution : MonotonicClock {
    private var p1 by RegularInt(0)
    private var p2 by RegularInt(0)
    private var c3 by RegularInt(0)
    private var c2 by RegularInt(0)
    private var c1 by RegularInt(0)

    override fun write(time: Time) {
        p1 = time.d1
        p2 = time.d2
        c3 = time.d3
        c2 = time.d2
        c1 = time.d1
    }

    override fun read(): Time {
        val i1 = c1
        val i2 = c2
        val i3 = c3
        val h1 = p2
        val h2 = p1

        var second by RegularInt(0)
        var third by RegularInt(0)

        if(i1 == h2 && i2 == h1){
            third = i3
        }
        if(i1 == h2){
            second = h1
        }
        return Time(h2, second, third)
    }
}