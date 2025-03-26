package util

fun main() {
   val a = subject<String>()
   val b = subject<Int>()

   a.zipWith(b)
      .forEach { (a, b) -> println("$a : $b") }

   a.emit("str0") // this will not be published
   a.emit("str1")
   b.emit(1)
   b.emit(2)
   b.emit(3)
   a.emit("str2")
   b.emit(4)
   a.emit("str3")
   b.emit(5)
}