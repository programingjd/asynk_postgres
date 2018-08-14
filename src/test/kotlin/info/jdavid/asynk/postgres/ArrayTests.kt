package info.jdavid.asynk.postgres

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class ArrayTests {

  @Test
  fun testInts() {
    val list = TextFormat.parse(Oids.IntArray, "{1,4,3}") as List<*>
    assertEquals(3, list.size)
    assertEquals(1, list[0])
    assertEquals(4, list[1])
    assertEquals(3, list[2])
  }

  @Test
  fun testFloats() {
    val list = TextFormat.parse(Oids.FloatArray, "{1.5,4,3.25}") as List<*>
    assertEquals(3, list.size)
    assertEquals(1.5f, list[0] as Float, .00001f)
    assertEquals(4f, list[1] as Float, .00001f)
    assertEquals(3.25f, list[2] as Float, .00001f)
  }

  @Test
  fun testStrings() {
    val list = TextFormat.parse(Oids.VarCharArray, "{\"a\",\"b\"}") as List<*>
    assertEquals(2, list.size)
    assertEquals("a", list[0])
    assertEquals("b", list[1])
  }

  @Test
  fun testStringsWithEscapes() {
    val list = TextFormat.parse(Oids.TextArray,
                                "{\"a\\\\n\\\"\",\"\\\\\\\\\",\"\\\\\\\"b\"}") as List<*>
    assertEquals(3, list.size)
    assertEquals("a\\n\"", list[0])
    assertEquals("\\\\", list[1])
    assertEquals("\\\"b", list[2])
  }

  @Test
  fun testArrayNumbers() {
    val list = TextFormat.parse(Oids.IntArray, "{{8,2,5},{1,2}}") as List<*>
    assertEquals(2, list.size)
    val first = list[0] as List<*>
    assertEquals(3, first.size)
    assertEquals(8, first[0])
    assertEquals(2, first[1])
    assertEquals(5, first[2])
    val second = list[1] as List<*>
    assertEquals(2, second.size)
    assertEquals(1, second[0])
    assertEquals(2, second[1])
  }

  @Test
  fun testArrayNoQuote() {
    val list = TextFormat.parse(Oids.TextArray, "{{a,b,c},{1,2}}") as List<*>
    assertEquals(2, list.size)
    val first = list[0] as List<*>
    assertEquals(3, first.size)
    assertEquals("a", first[0])
    assertEquals("b", first[1])
    assertEquals("c", first[2])
    val second = list[1] as List<*>
    assertEquals(2, second.size)
    assertEquals("1", second[0])
    assertEquals("2", second[1])
  }

  @Test
  fun testArrayWithQuotes() {
    val list = TextFormat.parse(Oids.TextArray, "{{\"a b\",\"b\",\"c\\\"\"},{\"1\",\"2\"}}") as List<*>
    assertEquals(2, list.size)
    val first = list[0] as List<*>
    assertEquals(3, first.size)
    assertEquals("a b", first[0])
    assertEquals("b", first[1])
    assertEquals("c\"", first[2])
    val second = list[1] as List<*>
    assertEquals(2, second.size)
    assertEquals("1", second[0])
    assertEquals("2", second[1])
  }

}
