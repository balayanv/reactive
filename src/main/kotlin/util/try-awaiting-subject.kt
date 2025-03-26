package util

fun main() {
   val stream = subject<Int>().waitForFirstSubscriber(1000)

   stream.emit(1)
   stream.emit(2)
   stream.emit(3)

   stream.subscribe { println(it) }

   stream.emit(4)
   stream.emit(5)
}