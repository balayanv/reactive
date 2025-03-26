package util

import java.util.concurrent.ForkJoinPool


fun main() {
   val stream = safeSubject<Int>()
   var count = 0

   stream
      .batch(sizeLimit = 1000)
      .subscribe {
         count += it.size
         "batch : ${it.size}, count : $count".logDebug()
      }

   for (i in 1..100) {
      fjp.submit {
         for (j in 1..1000) {
            stream.emit(j)
         }
      }
   }

   fjp.waitForTasksToFinish()
}