package info.jdavid.asynk.postgres

import java.math.BigInteger
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatter.ISO_LOCAL_DATE
import java.time.format.DateTimeFormatter.ISO_LOCAL_TIME
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.TemporalQuery
import java.util.Date
import java.util.Locale

internal object TextFormat {

  fun parse(typeDescription: String, value: String): Any {
    val index = typeDescription.indexOf(':')
    val oid = (if (index == -1) typeDescription else typeDescription.substring(0, index)).toInt()
    val type = Oids.fromOid(oid)
    return parse(type, value)
  }

  internal fun parse(type: Oids, value: String): Any {
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
      Oids.ByteArray -> parseByteArray(value)
      Oids.XML, Oids.Json, Oids.UUID -> value
      Oids.Oid -> parseShort(value)
      Oids.Bit, Oids.VarBit -> parseBitset(value)
      Oids.Point -> parsePoint(value)
      Oids.BooleanArray, Oids.ShortArray, Oids.IntArray, Oids.LongArray, Oids.FloatArray, Oids.DoubleArray,
      Oids.BigDecimalArray, Oids.OidArray, Oids.CharArray,
      Oids.DateArray,  Oids.TimestampArray, Oids.TimestampZArray,
      Oids.NameArray, Oids.TextArray, Oids.VarCharArray, Oids.BpCharArray,
      Oids.BitArray, Oids.VarBitArray, Oids.ByteArrayArray,
      Oids.UUIDArray, Oids.XMLArray  -> parseArray(value, type)
    }
  }

  fun format(value: Any): String {
    return when (value) {
      is Boolean -> formatBoolean(value)
      is ByteArray -> formatByteArray(value)
      is Date -> formatDate(value)
      is ZonedDateTime -> formatInstant(value.toInstant())
      is OffsetDateTime -> formatInstant(value.toInstant())
      is LocalDateTime -> formatInstant(value.toInstant(ZoneOffset.UTC))
      is LocalDate -> formatInstant(value.atStartOfDay(ZoneOffset.UTC).toInstant())
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

  fun parseDate(value: String): Date {
    val temporal = DateTimeFormatterBuilder().
      append(ISO_LOCAL_DATE).
      optionalStart().
      appendLiteral('T').
      optionalEnd().
      optionalStart().
      appendLiteral(' ').
      optionalEnd().
      optionalStart().
      append(ISO_LOCAL_TIME).
      optionalStart().
      appendOffsetId().
      optionalStart().
      appendLiteral('[').
      parseCaseSensitive().
      appendZoneRegionId().
      appendLiteral(']').
      toFormatter(Locale.US).parseBest(
        value,
        TemporalQuery<OffsetDateTime> { a -> OffsetDateTime.from(a) },
        TemporalQuery<LocalDateTime> { a -> LocalDateTime.from(a) },
        TemporalQuery<LocalDate> { a -> LocalDate.from(a) }
      )
    return Date.from(when (temporal) {
      is ZonedDateTime -> temporal.toInstant()
      is OffsetDateTime -> temporal.toInstant()
      is LocalDateTime -> temporal.toInstant(ZoneOffset.UTC)
      is LocalDate -> Instant.ofEpochSecond(temporal.atStartOfDay(ZoneOffset.UTC).toEpochSecond())
      else -> throw RuntimeException()
    })
  }

  fun parseTimestamp(value: String) = parseDate(value)

  fun parseTimestampZ(value: String) = parseDate(value)

  fun parseBitset(value: String): BigInteger {
    TODO()
  }

  fun parseByteArray(value: String): ByteArray {
    if (value.length < 2) throw IllegalArgumentException()
    if (value[0] != '\\' || value[1] != 'x') throw IllegalStateException(
      "Only hex format is supported for bytea data type."
    )
    return ByteArray(value.length / 2 - 1, {
      (hexDigit(value[it*2+2]).shl(4) + hexDigit(value[it*2+3])).toByte()
    })
  }

  fun parsePoint(value: String): Pair<Double, Double> {
    TODO()
  }

  fun <T: Oids> parseArray(value: String, type: T): Any {
    if (value.isEmpty() || value.first() != '{') return parse(type.arrayOf!!, value)
    if (value.length < 2) throw IllegalArgumentException()
    if (value.last() != '}') throw IllegalArgumentException()
    val list = mutableListOf<Any?>()
    var start = 1
    var i = 1
    var string = false
    while (true) {
      when (value[i]) {
        '{' -> i += consumeArray(value, i + 1)
        '"' -> {
          if (string) throw IllegalArgumentException()
          string = true
          i += consumeString(value, i + 1)
        }
        ',', '}' -> {
          val text = if (string) {
            if (start + 1 > i - 1) throw IllegalArgumentException()
            string = false
            string(value, start, i)
          } else value.substring(start, i)
          if (text == "NULL" || text == "null") list.add(null) else list.add(parse(type, text))
          start = i + 1
        }
      }
      if (++i == value.length) break
    }
    return list
  }

  private fun consumeArray(value: String, start: Int): Int {
    var i = start
    while (true) {
      when (value[i]) {
        '{' -> i += consumeArray(value, i + 1)
        '"' -> i += consumeString(value, i + 1)
        '}' -> return i - start + 1
      }
      if (++i == value.length) throw IllegalArgumentException()
    }
  }

  private fun consumeString(value: String, start: Int): Int {
    var escaped = false
    for (i in start until value.length) {
      escaped = when (value[i]) {
        '\\' -> !escaped
        '"' -> if (escaped) false else return i - start + 1
        else -> false
      }
    }
    throw IllegalArgumentException()
  }

  private fun string(value: String, start: Int, end: Int): String {
    var escaped = false
    val s = StringBuilder(end - start - 2)
    for (i in start + 1 until end - 1) {
      escaped = when (value[i]) {
        '\\' -> {
          if (escaped) s.append('\\')
          !escaped
        }
        else -> {
          s.append(value[i])
          false
        }
      }
    }
    return s.toString()
  }

  private fun hexDigit(c: Char): Int {
    if (c <= '9') return c - '0'
    if (c <= 'F') return c - 'A' + 10
    if (c <= 'f') return c - 'a' + 10
    throw IllegalArgumentException("Unexpected hex digit: ${c}")
  }

}
