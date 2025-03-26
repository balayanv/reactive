package util

fun main() {
   val s1 = subject<Int>()
   val s2 = subject<Int>()
   val s3 = subject<Int>()

   val j2 = s1.join(s2)

   check(j2 isNotSame s1) { "not the same as s1 is a regular subject" }

   val j3 = j2.join(s3)

   check(j3 isSame j2) { "must be the same since j2 was JoiningSubject" }

   j2.forEach {
      check(it == 1 || it == 2) { "condition failure" }
   }
   j3.forEach {
      check(it == 1 || it == 2 || it == 3) { "condition failure" }
   }


   for (i in 1..100) {

      when (rng().nextInt(1, 3)) {
         1 -> s1.emit(1)
         2 -> s2.emit(2)
         3 -> s3.emit(3)
         else -> throw IllegalStateException()
      }
   }
}
