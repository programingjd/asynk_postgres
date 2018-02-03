package info.jdavid.postgres

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.*

internal object TextFormat {

  fun format(value: Any): String {
    return when (value) {
      is Boolean -> formatBoolean(value)
      is ByteArray -> formatByteArray(value)
      is Date -> formatDate(value)
      is Instant -> formatInstant(value)
      is Iterable<*> -> formatArray(value)
      else -> value.toString()
    }
  }

  private val TRUE = "t"
  private val FALSE = "f"

  fun formatBoolean(bool: Boolean) = if (bool) TRUE else FALSE

  fun formatDate(date: Date) = formatInstant(date.toInstant())

  fun formatInstant(instant: Instant) = instant.
    atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

  fun formatByteArray(bytes: ByteArray) = "\\x${Message.hex(bytes).toUpperCase()}"

  fun formatArray(array: Iterable<*>) = array.
    map { it.toString().replace("\"", "\\\"") }.joinToString(",", "{", "}")

}
