package util

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.locks.Lock

inline fun <reified R> using(lock: Lock, block: () -> R): R {
   lock.lock()
   try {
      return block.invoke()
   } finally {
      lock.unlock()
   }
}

inline fun runIfFailsLogAndIgnore(crossinline block: () -> Unit) {
   try {
      return block.invoke()
   } catch (t: Throwable) {
      // do your own proper reporting
      // any exception reported from here is a serious bug
      // these should not ever be observed, but defensively you should ignore them
      // its not the reactive libraries fault if underlying consumer is buggy
      // the library is obligated to operate the chain of streams as normal
      t.printStackTrace()
   }
}

fun Any.log() = apply { this.logInfo() }

fun Any.logTrace() = apply {
//   logger().trace(Objects.toString(this))
   println(this)
}

fun Any.logDebug() = apply {
//   logger().debug(Objects.toString(this))
   println(this)
}

fun Any.logInfo() = apply {
//   logger().info(Objects.toString(this))
   println(this)
}

fun Any.logWarn() = apply {
//   logger().warn(Objects.toString(this))
   println(this)
}


fun nextPowerOf2BiggerThanOrEqual(n: Int): Int {
   var p = 1
   if (n > 0 && n and n - 1 == 0) return n
   while (p < n) p = p shl 1
   return p
}

// instead should be a time provider which will be configured depending on what you're doing
// in deterministic systems you dont just get currentime millis
fun now() : Long = System.currentTimeMillis()

// purely used for readability
typealias Millis = Long
val Int.ms: Millis get() = this.toLong()
val Int.millis: Millis get() = this.toLong()
val Int.milliseconds: Millis get() = this.toLong()

val Int.s: Millis get() = SECONDS.toMillis(this.toLong())

val Int.sec: Millis get() = SECONDS.toMillis(this.toLong())

val Int.second: Millis get() = SECONDS.toMillis(this.toLong())

val Int.seconds: Millis get() = SECONDS.toMillis(this.toLong())



fun imLucky(probability: Double = 0.5): Boolean {
   return ThreadLocalRandom.current().nextDouble() < probability
}

fun ForkJoinPool.awaitQuiescence(duration: Millis) {
   this.awaitQuiescence(duration, MILLISECONDS)
}

fun ForkJoinPool.waitForTasksToFinish() {
   this.awaitQuiescence(60.seconds)
}

val fjp = ForkJoinPool.commonPool()



fun sleepInterruptible(millis: Millis) {
   runIfFailsLogAndIgnore { Thread.sleep(millis) }
}

infix fun Any.isSame(that: Any) = this === that

infix fun Any.isNotSame(that: Any) = !isSame(that)

fun rng(): ThreadLocalRandom = ThreadLocalRandom.current()