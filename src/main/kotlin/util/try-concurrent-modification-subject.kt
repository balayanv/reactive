package util

fun main() {
   val stream = subject<Int>()

   stream.forEach {
      if (it == 3) {
         stream.subscribe {
            println("sub 2 : $it")
         }
      }
      "sub 1 : $it".log()
   }


   stream.emit(1)
   stream.emit(2)
   stream.emit(3)
   stream.emit(4)
   stream.emit(5)
}