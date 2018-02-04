package info.jdavid.postgres

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Date

internal object TextFormat {

  fun parse(typeDescription: String, value: String): Any {
    val index = typeDescription.indexOf(':')
    val oid = (if (index == -1) typeDescription else typeDescription.substring(0, index)).toInt()
    val type = Oids.fromOid(oid)
    return when (type) {
      Oids.Void -> throw RuntimeException()
      Oids.Boolean -> parseBoolean(value)
      Oids.Short -> parseShort(value)
      Oids.Int -> parseInt(value)
      Oids.Long -> parseLong(value)
      Oids.Float -> parseFloat(value)
      Oids.Double -> parseDouble(value)
      Oids.BigDecimal -> parseBigDecimal(value)
      Oids.Char -> value.apply { assert(length == 1) }[0]
      Oids.Date -> parseDate(value)
      Oids.Timestamp -> parseTimestamp(value)
      Oids.TimestampZ -> parseTimestampZ(value)
      Oids.Name, Oids.Text, Oids.VarChar, Oids.BpChar -> value
      Oids.XML, Oids.Json, Oids.UUID -> value
    }
  }

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
      is Sequence<*> -> formatArray(value)
      else -> value.toString()
    }
  }

  private val TRUE = "t"
  private val FALSE = "f"
  private val escape = { element: Any? ->
    element?.let {
      format(element).let {
        when (element) {
          is String -> "\"${it.replace("\"", "\\\"")}\""
          else -> it
        }
      }
    } ?: "NULL"
  }

  fun formatBoolean(bool: Boolean) = if (bool) TRUE else FALSE

  fun formatDate(date: Date) = formatInstant(date.toInstant())

  fun formatInstant(instant: Instant) = instant.
    atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

  fun formatByteArray(bytes: ByteArray) = "\\x${Message.hex(bytes).toUpperCase()}"

  fun formatArray(array: Sequence<*>) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: Iterable<*>) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: Array<*>) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: BooleanArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: ShortArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: IntArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: LongArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: FloatArray) = array.map(escape).joinToString(",", "{", "}")

  fun formatArray(array: DoubleArray) = array.map(escape).joinToString(",", "{", "}")

  fun parseShort(value: String) = value.toShort()

  fun parseInt(value: String) = value.toInt()

  fun parseLong(value: String) = value.toLong()

  fun parseFloat(value: String) = value.toFloat()

  fun parseDouble(value: String) = value.toDouble()

  fun parseBigDecimal(value: String) = value.toBigDecimal()

  fun parseBoolean(value: String) = value == TRUE

  fun parseDate(value: String) {
    TODO()
  }

  fun parseTimestamp(value: String) {
    TODO()
  }

  fun parseTimestampZ(value: String) {
    TODO()
  }

  fun <T: Oids> parseArray(value: String, type: T): List<Any> {
    TODO()
  }

}
