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
      is BooleanArray -> formatArray(value)
      is ShortArray -> formatArray(value)
      is IntArray -> formatArray(value)
      is LongArray -> formatArray(value)
      is FloatArray -> formatArray(value)
      is DoubleArray -> formatArray(value)
      is Array<*> -> formatArray(value)
      is Iterable<*> -> formatArray(value)
      else -> value.toString()
    }
  }

  private val TRUE = "t"
  private val FALSE = "f"
  private val escape = { element: Any? ->
    element?.let{ "\"${format(it).replace("\"", "\\\"")}\"" } ?: "NULL"
  }

  fun formatBoolean(bool: Boolean) = if (bool) TRUE else FALSE

  fun formatDate(date: Date) = formatInstant(date.toInstant())

  fun formatInstant(instant: Instant) = instant.
    atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

  fun formatByteArray(bytes: ByteArray) = "\\x${Message.hex(bytes).toUpperCase()}"

  fun formatArray(array: Iterable<*>) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: Array<*>) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: BooleanArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: ShortArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: IntArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: LongArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: FloatArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: DoubleArray) = array.map(escape).joinToString(",", "{", "}")



}
