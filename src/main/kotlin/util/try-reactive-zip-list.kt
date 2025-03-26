package util


fun main() {
   val streams = listOf<ISubject<Int>>(
      subject(),
      subject(),
      subject()
   )

   streams
      .zip()
      .doOnEach { "batch : $it".log() }
      .map { it.sum() }
      .forEach { "sum   : $it".log() }

   "after each stream has published once, we will see a batch".log()
   streams[0].emit(1)
   streams[1].emit(1)
   streams[2].emit(1)

   "now any update on any of the streams creates a new batch".log()
   streams[0].emit(2)
   streams[1].emit(2)
   streams[2].emit(5)
}