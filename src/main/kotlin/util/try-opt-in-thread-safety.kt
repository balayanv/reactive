package util

import java.util.concurrent.ForkJoinPool


private fun parallelEmittersUnsafeSubject() {
   var count = 0

   val subject = subject<Int>()

   subject.subscribe {
      if (imLucky(1 / 10000.0)) "increment".logDebug()
      count += it
   }

   for (i in 1..10) {
      ForkJoinPool.commonPool().execute {
         for (j in 1..100000) {
            subject.emit(1)
         }
      }
   }

   ForkJoinPool.commonPool().waitForTasksToFinish()

   "this should not work because we are clearly executing increments in different threads without synchronizing".logWarn()
   "count : $count/1000000".logWarn()
}

private fun parallelEmittersSafeSubject() {
   var count = 0

   val subject = safeSubject<Int>()

   subject.subscribe {
      if (imLucky(1 / 10000.0)) "increment".logDebug()
      count += it
   }

   for (i in 1..10) {
      fjp.submit {
         for (j in 1..100000) {
            subject.emit(1)
         }
      }
   }

   fjp.waitForTasksToFinish()

   "this shall work because we are using synchronized subject".logWarn()
   "which means direct consumers are guarded by the subjects lock".logWarn()
   "this is useful if you know that you need to be safe on subject level".logWarn()
   "which means 1 lock per subject".logWarn()
   "count : $count/1000000".log()
}

private fun parallelEmittersSafeSubscriber() {
   var count = 0

   val subject = subject<Int>()

   subject.subscribeSafe {
      if (imLucky(1 / 10000.0)) "increment".logDebug()
      count += it
   }

   for (i in 1..10) {
      fjp.submit {
         for (j in 1..100000) {
            subject.emit(1)
         }
      }
   }

   fjp.waitForTasksToFinish()

   "this shall work because we are using synchronized subscriber".logWarn()
   "which means subscribers that wrap the consumer closure are synchronized".logWarn()
   "this is useful if you know that you need to be safe per consumer not per subject".logWarn()
   "which means 1 lock per subscriber (for single subscriber its effectively the same as safe subject)".logWarn()
   "count : $count/1000000".log()
}

private fun singleEmiterConcurrentSubjectUnsafeConsumer() {
   var count = 0

   val subject = subject<Int>()

   subject
      .executeOn(fjp) // now we have parallel observable
      .subscribe {
         // which means any of the FJP threads can be here
         if (imLucky(1 / 10000.0)) "increment".logDebug()
         count += it
      }

   for (i in 1..1000000) {
      subject.emit(1)
   }

   fjp.waitForTasksToFinish()

   "this should not work because we are invoking unsafe subscriber 10000000 times concurrently".logWarn()
   "count : $count/1000000".logWarn()
}

private fun singleEmitterConcurrentSubjectSafeConsumer() {
   var count = 0

   val subject = subject<Int>()

   subject
      .executeOn(fjp) // now we have parallel observable
      .subscribeSafe {
         if (imLucky(1 / 10000.0)) "increment".logDebug()
         count += it
      }

   for (i in 1..1000000) {
      subject.emit(1)
   }

   fjp.waitForTasksToFinish()

   "this should work because we are invoking unsafe subscriber 10000000 times concurrently".logWarn()
   "but subscriber is synchronizing before executing its code...".logWarn()
   "the benefit of opt in safety is that now that you know that the subscriber is safe".logWarn()
   "all the downstream operations filter, map, reduce are safe too".logWarn()
   "it gives you control over when and what does the synchronization".logWarn()
   "count : $count/1000000".log()
}

private fun singleEmitterConcurrentSubjectSafeSubject() {
   var count = 0

   val concurrentSubject = subject<Int>()
      .executeOn(fjp)

   val safeObservable = concurrentSubject
      .safe() // now we have safe subject

   safeObservable
      .subscribe {
         if (imLucky(1 / 10000.0)) "increment".logDebug()
         count += it
      }

   // obviously you dont need to break the method chaining this is just for demo

   for (i in 1..1000000) {
      subject<Int>().emit(1)
   }

   fjp.waitForTasksToFinish()

   "this should work because we are invoking unsafe subscriber 10000000 times concurrently".logWarn()
   "but then we pass them trough safe subject and then subscribing with unsafe subscriber".logWarn()
   "count : $count/1000000".log()
}

fun main() {
//   parallelEmittersUnsafeSubject()
//   parallelEmittersSafeSubject()
//   parallelEmittersSafeSubscriber()
//   singleEmiterConcurrentSubjectUnsafeConsumer()
//   singleEmitterConcurrentSubjectSafeConsumer()
   singleEmitterConcurrentSubjectSafeSubject()
}