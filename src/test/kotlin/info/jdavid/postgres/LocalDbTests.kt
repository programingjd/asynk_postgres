package info.jdavid.postgres

import info.jdavid.sql.use
import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test
import org.junit.Assert.*

class LocalDbTests {
  private val credentials: PostgresAuthentication.Credentials
  private val databaseName: String
  init {
    val properties = Utils.properties()
    val username = properties.getProperty("postgres_username") ?: "postgres"
    val password = properties.getProperty("postgres_password") ?: "postgres"
    credentials = PostgresAuthentication.Credentials.PasswordCredentials(username, password)
    databaseName = properties.getProperty("postgres_database") ?: "postgres"
  }

  @Test
  fun testSimple() {
    runBlocking {
      credentials.connectTo(databaseName).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              name           text      NOT NULL,
              active         boolean   DEFAULT FALSE NOT NULL,
              creation_date  date      DEFAULT current_timestamp
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (name) VALUES (?)
          """.trimIndent(),
          listOf("Name1")
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (name, active) VALUES (?, ?)
          """.trimIndent(),
          listOf("Name2", true)
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (name, active) VALUES (?, ?)
          """.trimIndent(),
          listOf("Name3", false)
        ))
        it.rows(
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
        assertEquals(2, it.affectedRows(
          """
            UPDATE test SET active=TRUE WHERE active=FALSE
          """.trimIndent()
        ))
        it.rows(
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
        assertEquals(2, it.affectedRows(
          """
            UPDATE test SET active=FALSE WHERE NOT(name LIKE '%2')
          """.trimIndent()
        ))
        it.rows(
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
        assertEquals(2, it.affectedRows(
          """
              DELETE FROM test WHERE active=FALSE
            """
        ))
        it.rows(
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
      credentials.connectTo(databaseName).use {
        assertEquals(0, it.affectedRows(
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
          assertEquals(1, affectedRows(listOf("Name1")))
          assertEquals(1, affectedRows(listOf("Name2")))
          assertEquals(1, affectedRows(listOf("Name3")))
          assertEquals(1, affectedRows(listOf("Name4")))
          aClose()
          try {
            affectedRows(listOf("Name5"))
            fail("Execution of closed prepared statement should have failed.")
          }
          catch (ignore: Exception) {}
        }
        it.prepare("SELECT * FROM test WHERE active=?").apply {
          assertNotNull(name)
          assertEquals(4, rows(listOf(false)).toList().size)
          assertEquals(0, rows(listOf(true)).toList().size)
          assertEquals(1, it.affectedRows("DELETE FROM test WHERE name=?", listOf("Name4")))
          assertEquals(3, rows(listOf(false)).toList().size)
          assertEquals(1, it.affectedRows("UPDATE test SET active=TRUE WHERE name=?", listOf("Name1")))
          assertEquals(2, rows(listOf(false)).toList().size)
          aClose()
          try {
            rows(listOf(false))
            fail("Execution of closed prepared statement should have failed.")
          }
          catch (ignore: Exception) {}
        }
      }
    }
  }

  @Test
  fun testNull() {
    runBlocking {
      credentials.connectTo(databaseName).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              i1             integer,
              s1             text      DEFAULT NULL,
              b1             boolean
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i1, s1, b1) VALUES (?, ?, ?)
          """.trimIndent(),
          listOf(1234, null, true)
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i1, b1) VALUES (?, ?)
          """.trimIndent(),
          listOf(null, false)
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i1, b1) VALUES (?, ?)
          """.trimIndent(),
          listOf(null, null)
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals(1234, get(0)["i1"])
          assertNull(get(0)["s1"])
          assertEquals(true, get(0)["b1"])
          assertNull(get(1)["i1"])
          assertNull(get(1)["s1"])
          assertEquals(false,get(1)["b1"])
          assertNull(get(2)["i1"])
          assertNull(get(2)["s1"])
          assertNull(get(2)["b1"])
        }
      }
    }
  }

  @Test
  fun testStrings() {
    runBlocking {
      credentials.connectTo(databaseName).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              s1             text      DEFAULT NULL
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (s1) VALUES (?)
          """.trimIndent(),
          listOf("\n")
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (s1) VALUES (?)
          """.trimIndent(),
          listOf("\\n")
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (s1) VALUES (?)
          """.trimIndent(),
          listOf("\"")
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(3, size)
          assertEquals("\n", get(0)["s1"])
          assertEquals("\\n", get(1)["s1"])
          assertEquals("\"",get(2)["s1"])
        }
      }
    }
  }

  @Test
  fun testBytes() {
    runBlocking {
      credentials.connectTo(databaseName).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              data           bytea
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (data) VALUES (?)
          """.trimIndent(),
          listOf(byteArrayOf(0x01, 0x02, 0x03, 0x04))
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          println(this)
          assertEquals(1, size)
          val bytes = get(0)["data"] as ByteArray
          assertEquals(4, bytes.size)
          assertEquals(1.toByte(), bytes[0])
          assertEquals(2.toByte(), bytes[1])
          assertEquals(3.toByte(), bytes[2])
        }
      }
    }
  }

  @Test
  fun testArrays() {
    runBlocking {
      credentials.connectTo(databaseName).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              i1             integer[],
              s1             text[][],
              b1             boolean[]
            )
          """.trimIndent()
        ))
        assertEquals(1, it.affectedRows(
          """
            INSERT INTO test (i1, s1, b1) VALUES (?, ?, ?)
          """.trimIndent(),
          listOf(arrayOf(1234), arrayOf(arrayOf("a", "b", "c"), arrayOf("1", "2", "3")), arrayOf(true, true))
        ))
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          println(this)
          assertEquals(1, size)
          val ints = get(0)["i1"] as List<*>
          assertEquals(1, ints.size)
          assertEquals(1234, ints[0])
          val texts = get(0)["s1"] as List<*>
          assertEquals(2, texts.size)
          val first = texts[0] as List<*>
          val second = texts[1] as List<*>
          assertEquals(3, first.size)
          assertEquals("a", first[0])
          assertEquals("b", first[1])
          assertEquals("c", first[2])
          assertEquals(3, second.size)
          assertEquals("1", second[0])
          assertEquals("2", second[1])
          assertEquals("3", second[2])
          val bools = get(0)["b1"] as List<*>
          assertEquals(2, bools.size)
          assertEquals(true, bools[0])
          assertEquals(true, bools[1])
        }
      }
    }
  }

}
