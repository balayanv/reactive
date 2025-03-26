package util



fun main() {
   val stream = safeSubject<Int>()
   var count = 0

   stream
      .doOnEach { "<< $it".log() }
      .batch(1.second)
      .subscribe {
         count += it.size
         "batch : ${it.size}, count : $count".logDebug()
      }

   for (i in 1..100) {
      ">> $i".log()
      stream.emit(i)
      sleepInterruptible(500.ms)

      if (i % 10 == 0) {
         "long pause".log()
         sleepInterruptible(4.sec)
      }
   }

   readLine()
}