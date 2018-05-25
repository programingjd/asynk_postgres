package info.jdavid.asynk.postgres

import org.junit.Test
import org.junit.Assert.*
import java.math.BigDecimal
import java.math.RoundingMode

class NumericTests {

  @Test
  fun testShort() {
    assertEquals(123.toShort(), TextFormat.parseShort("123"))
    assertEquals("123", TextFormat.format(123.toShort()))
  }

  @Test
  fun testInt() {
    assertEquals(12345, TextFormat.parseInt("12345"))
    assertEquals("12345", TextFormat.format(12345))
  }

  @Test
  fun testLong() {
    assertEquals(12345678900L, TextFormat.parseLong("12345678900"))
    assertEquals("12345678900", TextFormat.format(12345678900L))
  }

  @Test
  fun testFloat() {
    assertEquals(1.23f, TextFormat.parseFloat("1.23"), .00001f)
    assertEquals("1.23", TextFormat.format(1.23f))
  }

  @Test
  fun testDouble() {
    assertEquals(1.23, TextFormat.parseDouble("1.23"), .00000001)
    assertEquals("1.23", TextFormat.format(1.23))
  }

  @Test
  fun testBigDecimal() {
    assertEquals(BigDecimal(1.23456).setScale(8, RoundingMode.HALF_UP),
                 TextFormat.parseBigDecimal("1.23456").setScale(8, RoundingMode.HALF_UP))
    assertEquals("1.23456", TextFormat.format(BigDecimal(1.23456).
      setScale(8, RoundingMode.HALF_UP).stripTrailingZeros()))
  }

}
