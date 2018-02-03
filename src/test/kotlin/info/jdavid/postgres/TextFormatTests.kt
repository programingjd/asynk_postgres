package info.jdavid.postgres

import org.junit.Test

import org.junit.Assert.*
import java.security.MessageDigest
import java.util.*

class TextFormatTests {

  @Test
  fun testBoolean() {
    assertEquals("t", TextFormat.formatBoolean(true))
    assertEquals("t", TextFormat.format(true))
    assertEquals("f", TextFormat.formatBoolean(false))
    assertEquals("f", TextFormat.format(false))
  }

  @Test
  fun testBlob() {
    val bytes = randomBytes("testBlob".toByteArray())
    val expected = "\\x${Message.hex(bytes).toUpperCase()}"
    assertEquals(expected, TextFormat.formatByteArray(bytes))
    assertEquals(expected, TextFormat.format(bytes))
  }

  @Test
  fun testDate() {
    val date = Date(1517668304158L)
    assertEquals("2018-02-03T14:31:44.158Z", TextFormat.formatDate(date))
    assertEquals("2018-02-03T14:31:44.158Z", TextFormat.format(date))
    assertEquals("2018-02-03T14:31:44.158Z", TextFormat.format(date.toInstant()))
    assertEquals("2018-02-03T14:31:44.158Z", TextFormat.format(date.toInstant()))
  }

  @Test
  fun testMixedArray() {
    val array = arrayOf("abc", "\"quoted\"", 123, true, 1.5, null)
    val expected = "{\"abc\",\"\\\"quoted\\\"\",\"123\",\"t\",\"1.5\",NULL}"
    assertEquals(expected, TextFormat.formatArray(array))
    assertEquals(expected, TextFormat.formatArray(array))
  }

  private fun randomBytes(seed: ByteArray, steps: Int = 4): ByteArray {
    val md5 = MessageDigest.getInstance("MD5")
    val data = ByteArray(steps * 16)
    for (i in 0 until steps) {
      md5.update(seed)
      md5.update(data, 0, i * 16)
      System.arraycopy(md5.digest(), 0, data, i * 16, 16)
    }
    return data
  }

}
