package util


fun main() {
   val subject = subject<Int>().replayForNewSubscribers(1000)

   subject.emit(1)
   subject.emit(2)

   subject.forEach { "subscriber 1 : $it".log() }

   subject.emit(3)

   subject.forEach { "subscriber 2 : $it".log() }

   subject.emit(4)
   subject.emit(5)
   subject.emit(6)
}