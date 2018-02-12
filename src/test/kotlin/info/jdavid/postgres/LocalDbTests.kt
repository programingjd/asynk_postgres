package info.jdavid.postgres

import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test
import org.junit.Assert.*

class LocalDbTests {

  @Test
  fun testSimple() {
    runBlocking {
      Authentication.Credentials.PasswordCredentials().connectTo("postgres").use {
        assertEquals(0, it.update(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              name           text      NOT NULL,
              active         boolean   DEFAULT FALSE NOT NULL,
              creation_date  date      DEFAULT current_timestamp
            )
          """.trimIndent()
        ))
        assertEquals(1, it.update(
          """
            INSERT INTO test (name) VALUES (?)
          """.trimIndent(),
          listOf("Name1")
        ))
        assertEquals(1, it.update(
          """
            INSERT INTO test (name, active) VALUES (?, ?)
          """.trimIndent(),
          listOf("Name2", true)
        ))
        assertEquals(1, it.update(
          """
            INSERT INTO test (name, active) VALUES (?, ?)
          """.trimIndent(),
          listOf("Name3", false)
        ))
        it.query(
          """
            SELECT * FROM test ORDER BY name
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals("Name1", get(0)["name"])
          assertFalse(get(0)["active"] as Boolean)
          assertEquals("Name2", get(1)["name"])
          assertTrue(get(1)["active"] as Boolean)
          assertEquals("Name3", get(2)["name"])
          assertFalse(get(2)["active"] as Boolean)
        }
        assertEquals(2, it.update(
          """
            UPDATE test SET active=TRUE WHERE active=FALSE
          """.trimIndent()
        ))
        it.query(
          """
            SELECT * FROM test ORDER BY name
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals("Name1", get(0)["name"])
          assertTrue(get(0)["active"] as Boolean)
          assertEquals("Name2", get(1)["name"])
          assertTrue(get(1)["active"] as Boolean)
          assertEquals("Name3", get(2)["name"])
          assertTrue(get(2)["active"] as Boolean)
        }
        assertEquals(2, it.update(
          """
            UPDATE test SET active=FALSE WHERE NOT(name LIKE '%2')
          """.trimIndent()
        ))
        it.query(
          """
            SELECT * FROM test ORDER BY name
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals("Name1", get(0)["name"])
          assertFalse(get(0)["active"] as Boolean)
          assertEquals("Name2", get(1)["name"])
          assertTrue(get(1)["active"] as Boolean)
          assertEquals("Name3", get(2)["name"])
          assertFalse(get(2)["active"] as Boolean)
        }
        assertEquals(2, it.update(
          """
              DELETE FROM test WHERE active=FALSE
            """
        ))
        it.query(
          """
            SELECT * FROM test ORDER BY name
          """.trimIndent()
        ).toList().apply {
          assertEquals(1, size)
          assertEquals("Name2", get(0)["name"])
          assertTrue(get(0)["active"] as Boolean)
        }
      }
    }
  }

  @Test
  fun testPreparedStatement() {
    runBlocking {
      Authentication.Credentials.PasswordCredentials().connectTo("postgres").use {
        assertEquals(0, it.update(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              name           text      NOT NULL,
              active         boolean   DEFAULT FALSE NOT NULL,
              creation_date  date      DEFAULT current_timestamp
            )
          """.trimIndent()
        ))
        it.prepare("""
            INSERT INTO test (name) VALUES (?)
          """.trimIndent()).apply {
          assertNotNull(name)
          assertEquals(1, update(listOf("Name1")))
          assertEquals(1, update(listOf("Name2")))
          assertEquals(1, update(listOf("Name3")))
          assertEquals(1, update(listOf("Name4")))
          close()
          try {
            update(listOf("Name5"))
            fail("Execution of closed prepared statement should have failed.")
          }
          catch (ignore: Exception) {}
        }
        it.prepare("SELECT * FROM test WHERE active=?").apply {
          assertNotNull(name)
          assertEquals(4, query(listOf(false)).toList().size)
          assertEquals(0, query(listOf(true)).toList().size)
          assertEquals(1, it.update("DELETE FROM test WHERE name=?", listOf("Name4")))
          assertEquals(3, query(listOf(false)).toList().size)
          assertEquals(1, it.update("UPDATE test SET active=TRUE WHERE name=?", listOf("Name1")))
          assertEquals(2, query(listOf(false)).toList().size)
          close()
          try {
            query(listOf(false))
            fail("Execution of closed prepared statement should have failed.")
          }
          catch (ignore: Exception) {}
        }
      }
    }
  }

}
