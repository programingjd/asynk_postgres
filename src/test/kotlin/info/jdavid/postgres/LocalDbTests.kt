package info.jdavid.postgres

import kotlinx.coroutines.experimental.runBlocking
import org.junit.Test

class LocalDbTests {

  @Test
  fun test1() {
    runBlocking {
      Authentication.Credentials.PasswordCredentials().connectTo("postgres").use {
        println(it.update(
          """
            CREATE TEMPORARY TABLE test (
              id             serial    PRIMARY KEY,
              name           text      NOT NULL,
              active         boolean   DEFAULT FALSE NOT NULL,
              creation_date  date      DEFAULT current_timestamp
            )
          """.trimIndent()
        ))
        println(it.update(
          """
            INSERT INTO test (name) VALUES (?)
          """.trimIndent(),
          listOf("Name1")
        ))
        println(it.update(
          """
            INSERT INTO test (name, active) VALUES (?, ?)
          """.trimIndent(),
          listOf("Name2", true)
        ))
        println(it.query(
          """
            SELECT * FROM test
          """.trimIndent()
        ))
      }
    }
  }

}
