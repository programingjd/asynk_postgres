package info.jdavid.postgres

import org.junit.Test
import org.junit.Assert.*

class DateTests {

  @Test
  fun test() {
    assertEquals("2010-01-15T14:30:01Z", parseAndFormat("2010-01-15T14:30:01Z"))
    assertEquals("2010-01-15T14:30:01Z", parseAndFormat("2010-01-15T14:30:01"))
    assertEquals("2010-01-15T00:00:00Z", parseAndFormat("2010-01-15"))
    assertEquals("2010-01-15T14:30:01Z", parseAndFormat("2010-01-15T14:30:01.000Z"))
    assertEquals("2010-01-15T14:30:01Z", parseAndFormat("2010-01-15T14:30:01.000"))
    assertEquals("2010-01-15T14:30:01Z", parseAndFormat("2010-01-15 14:30:01Z"))
    assertEquals("2010-01-15T14:30:01Z", parseAndFormat("2010-01-15 14:30:01"))
    assertEquals("2010-01-15T14:30:01Z", parseAndFormat("2010-01-15 14:30:01.000Z"))
    assertEquals("2010-01-15T14:30:01Z", parseAndFormat("2010-01-15 14:30:01.000"))
  }

  private fun parseAndFormat(dateString: String) = TextFormat.format(TextFormat.parseDate(dateString))

}
