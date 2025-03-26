package util

fun main() {
   val s1 = subject<Int>()
   val s2 = subject<Int>()

   s1.forEach { "s1 : $it".log() }
   s2.forEach { "s2 : $it".log() }

   val s3 = emitLastUnseenValuesOf(s1, s2)
   s3.forEach { "s3 : $it".log() }

   s1.emit(1)
   s1.emit(2)

   s1.emit(3)
   s2.emit(4)

   s2.emit(5)

   s2.emit(6)
   s1.emit(7)

   s1.emit(8)
   s2.emit(9)

}