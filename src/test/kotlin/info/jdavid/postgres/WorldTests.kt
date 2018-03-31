package info.jdavid.postgres

import info.jdavid.sql.use
import kotlinx.coroutines.experimental.runBlocking
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.util.regex.Pattern

class WorldTests {

  private val credentials: PostgresAuthentication.Credentials
  private val databaseName: String
  init {
    val properties = Utils.properties()
    val username = properties.getProperty("postgres_username") ?: "postgres"
    val password = properties.getProperty("postgres_password") ?: "postgres"
    credentials = PostgresAuthentication.Credentials.PasswordCredentials(username, password)
    databaseName = properties.getProperty("postgres_database") ?: "postgres"
  }

  @Before
  fun createTables() {
    val sql = WorldTests::class.java.getResource("/world.sql").readText()
    val split = sql.split(Pattern.compile(";\\s*\\r?\\n"))
    runBlocking {
      credentials.connectTo(databaseName).use { connection ->
        connection.withTransaction {
          split.forEachIndexed { i: Int, it: String ->
            if (it.trim().isEmpty()) return@forEachIndexed
            connection.affectedRows(it)
          }
        }
      }
    }
  }

  @Test
  fun testData() {
    runBlocking {
      credentials.connectTo(databaseName).use { connection ->
        Assert.assertEquals(
          239L,
          connection.rows("SELECT count(*) AS count FROM country").toList().first()["count"]
        )
        val codes = connection.rows(
          "SELECT DISTINCT CountryCode AS code FROM city"
        ).toList().map { it["code"] }
        Assert.assertEquals(232, codes.size)
        Assert.assertTrue(codes.contains("FRA"))
        Assert.assertEquals(codes.toSet().size, codes.size)
        val all = connection.rows(
          "SELECT Code AS code FROM country"
        ).toList().map { it["code"] }.toSet()
        Assert.assertEquals(239, all.size)
        Assert.assertEquals(all.toSet().size, all.size)
        Assert.assertTrue(all.containsAll(codes))

        val cities = connection.rows(
          "SELECT Name AS city FROM city WHERE CountryCode=? ORDER BY Population DESC",
          listOf("FRA")
        ).toList().map { it["city"] }
        Assert.assertEquals(40, cities.size)
        Assert.assertEquals("Paris", cities[0])
        Assert.assertEquals("Marseille", cities[1])
        Assert.assertEquals("Lyon", cities[2])
      }
    }
  }

  @After
  fun dropTables() {
    runBlocking {
      credentials.connectTo(databaseName).use { connection ->
        connection.affectedRows("DROP TABLE IF EXISTS countrylanguage,city,country")
      }
    }
  }

}
