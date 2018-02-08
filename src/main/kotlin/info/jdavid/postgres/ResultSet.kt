package info.jdavid.postgres

import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.createCoroutine
import kotlin.coroutines.experimental.suspendCoroutine

interface ResultSet {

  operator fun iterator(): RowIterator

  interface RowIterator {
    suspend operator fun hasNext(): Boolean
    suspend operator fun next(): Map<String, Any?>
  }

  companion object {
    internal interface RowFetcher {
      suspend fun yieldAll(values: List<Map<String, Any?>>?)
    }
    internal class RowIteratorCoroutine(
      override val context: CoroutineContext,
      fetcher: suspend RowFetcher.() -> Unit
    ): RowIterator, RowFetcher, Continuation<Unit> {
      val available = mutableListOf<Map<String, Any?>>()
      var done = false
      var cont = fetcher.createCoroutine(this, this)

      override suspend fun hasNext(): Boolean {
        if (!done) {
          if (available.isEmpty()) {
            fetch()
            return !done
          }
          return true
        }
        return false
      }

      override suspend fun next(): Map<String, Any?> {
        if (done) throw NoSuchElementException()
        if (available.isEmpty()) {
          fetch()
          if (done) throw NoSuchElementException()
        }
        return available.removeAt(0)
      }

      suspend fun fetch() = suspendCoroutine<Unit> { c ->
        val cont = this.cont
        this.cont = c
        cont.resume(Unit)
      }

      override suspend fun yieldAll(values: List<Map<String, Any?>>?): Unit = suspendCoroutine { c ->
        if (values == null) done = true else available.addAll(values)
        val cont = this.cont
        this.cont = c
        cont.resume(Unit)
      }

      override fun resume(value: Unit) {
        cont.resume(Unit)
      }

      override fun resumeWithException(exception: Throwable) {
        done = true
        cont.resumeWithException(exception)
      }

    }

    internal fun resultSet(context: CoroutineContext,
                           fetcher: suspend RowFetcher.() -> Unit): ResultSet {
      return object : ResultSet {
        override fun iterator() = rowIterator(context, fetcher)
      }
    }
    internal fun rowIterator(context: CoroutineContext,
                             fetcher: suspend RowFetcher.() -> Unit): RowIterator {
      return RowIteratorCoroutine(context, fetcher)
    }
  }

}
