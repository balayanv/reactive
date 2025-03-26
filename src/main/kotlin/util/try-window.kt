package util

fun main() {
   val subject = subject<Int>()

   subject
      .window(3)
      .forEach { it.log() }

   for (i in 0..10) {
      subject.emit(i)
   }


}