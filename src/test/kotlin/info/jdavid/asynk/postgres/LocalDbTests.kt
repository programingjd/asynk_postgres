package info.jdavid.asynk.postgres

import info.jdavid.asynk.sql.use
import kotlinx.coroutines.experimental.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class LocalDbTests {
  private val credentials: PostgresAuthentication.Credentials
  private val databaseName: String
  init {
    credentials = PostgresAuthentication.Credentials.PasswordCredentials("test", "asynk")
    databaseName = "world"
  }

  companion object {
    @JvmStatic @BeforeAll
    fun startDockerContainers() {
      Executors.newCachedThreadPool().apply {
        Docker.DatabaseVersion.values().forEach {
          submit {
            Docker.startContainer(it)
          }
        }
        shutdown()
      }.awaitTermination(300, TimeUnit.SECONDS)
    }
    @JvmStatic @AfterAll
    fun stopDockerContainers() {
      Executors.newCachedThreadPool().apply {
        Docker.DatabaseVersion.values().forEach {
          submit {
            Docker.stopContainer(it)
          }
        }
        shutdown()
      }.awaitTermination(15, TimeUnit.SECONDS)
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testSimple(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
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

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testPreparedStatement(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
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
          this.aClose()
          try {
            affectedRows(listOf("Name5"))
            fail("Execution of closed prepared statement should have failed.")
          }
          catch (ignore: Exception) {}
        }
        val s1 = it.prepare("SELECT * FROM test WHERE active=?")
        assertNotNull(s1.name)
        assertEquals(4, s1.rows(listOf(false)).toList().size)
        assertEquals(0, s1.rows(listOf(true)).toList().size)
        val s2 = it.prepare("SELECT * FROM test WHERE name=?")
        assertNotNull(s2.name)
        assertEquals(1, s2.rows(listOf("Name4")).toList().size)
        assertEquals(1, it.affectedRows("DELETE FROM test WHERE name=?", listOf("Name4")))
        assertEquals(3, s1.rows(listOf(false)).toList().size)
        assertEquals(0, s2.rows(listOf("Name4")).toList().size)
        assertEquals(1, it.affectedRows("UPDATE test SET active=TRUE WHERE name=?", listOf("Name1")))
        assertEquals(2, s1.rows(listOf(false)).toList().size)
        s1.aClose()
        assertEquals(1, s2.rows(listOf("Name1")).toList().size)
        try {
          s1.rows(listOf(false))
          fail<Nothing>("Execution of closed prepared statement should have failed.")
        }
        catch (ignore: Exception) {}
        s2.aClose()
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testNull(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
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

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testStrings(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
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

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testBytes(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
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

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testArrays(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
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

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testTransactions(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use {
        assertEquals(0, it.affectedRows(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              v              integer   NOT NULL DEFAULT 0
            )
          """.trimIndent()
        ))
        it.withTransaction {
          assertEquals(1, it.affectedRows(
            """
            INSERT INTO test (v) VALUES (?)
          """.trimIndent(),
            listOf(34)
          ))
          assertEquals(1, it.affectedRows(
            """
            INSERT INTO test (v) VALUES (?)
          """.trimIndent(),
            listOf(35)
          ))
        }
        try {
          it.withTransaction {
            assertEquals(1, it.affectedRows(
              """
            INSERT INTO test (v) VALUES (?)
          """.trimIndent(),
              listOf(52)
            ))
            throw RollbackException()
          }
        }
        catch (ignore: RollbackException) {}
        it.rows(
          """
            SELECT * FROM test ORDER BY id
          """.trimIndent()
        ).toList().apply {
          assertEquals(2, size)
          assertEquals(34, get(0)["v"])
          assertEquals(35, get(1)["v"])
        }
      }
    }
  }

  @ParameterizedTest(name = "{index} => {0}")
  @EnumSource(value = Docker.DatabaseVersion::class)
  fun testWorldData(databaseVersion: Docker.DatabaseVersion) {
    runBlocking {
      val address = InetSocketAddress(InetAddress.getLocalHost(), databaseVersion.port)
      credentials.connectTo(databaseName, address).use { connection ->
        assertEquals(
          239L,
          connection.rows("SELECT count(*) AS count FROM country").toList().first()["count"]
        )
        val codes = connection.rows(
          "SELECT DISTINCT CountryCode AS code FROM city"
        ).toList().map { it["code"] }
        assertEquals(232, codes.size)
        assertTrue(codes.contains("FRA"))
        assertEquals(codes.toSet().size, codes.size)
        val all = connection.rows(
          "SELECT Code AS code FROM country"
        ).toList().map { it["code"] }.toSet()
        assertEquals(239, all.size)
        assertEquals(all.toSet().size, all.size)
        assertTrue(all.containsAll(codes))

        val cities = connection.rows(
          "SELECT Name AS city FROM city WHERE CountryCode=? ORDER BY Population DESC",
          listOf("FRA")
        ).toList().map { it["city"] }
        assertEquals(40, cities.size)
        assertEquals("Paris", cities[0])
        assertEquals("Marseille", cities[1])
        assertEquals("Lyon", cities[2])
      }
    }
  }

  private class RollbackException: RuntimeException()

}
