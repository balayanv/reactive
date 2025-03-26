package util

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

fun main() {
   val someOrigin = subject<Double>()

   Executors
      .newSingleThreadScheduledExecutor()
      .scheduleAtFixedRate(
         { someOrigin.emit(rng().nextDouble()) }, 0, 500, TimeUnit.MILLISECONDS
      )

   "press enter to subscribe".log()
   readLine()

   val subscriber = subscriber<Double> {
      println("consumer : $it")
   }

   someOrigin.subscribe(subscriber)


   "press enter to unsubscribe".log()
   readLine()

   someOrigin.unsubscribe(subscriber)

   "press enter to terminate the example".log()
   readLine()
}