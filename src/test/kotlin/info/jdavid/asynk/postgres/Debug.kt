package info.jdavid.asynk.postgres

import info.jdavid.asynk.sql.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.InetSocketAddress

class Debug {

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      Docker.DatabaseVersion.values().last().let { version ->
        Docker.startContainer(version)
      try {
          val credentials =
            PostgresAuthentication.Credentials.PasswordCredentials("test", "asynk")
          val databaseName = "world"
          val address = InetSocketAddress(InetAddress.getLocalHost(), version.port)
          runBlocking {
            launch(Dispatchers.IO) {
              credentials.connectTo(databaseName, address).use { connection ->
                println("creating table")
                connection.affectedRows(
                  """
                    CREATE TEMPORARY TABLE test (
                      id             serial    PRIMARY KEY,
                      name           text      NOT NULL,
                      bytes          bytea      DEFAULT NULL,
                      active         boolean   DEFAULT FALSE NOT NULL,
                      creation_date  date      DEFAULT current_timestamp
                    )
                  """.trimIndent()
                )
                println("adding rows")
                (1..100).forEach {
                  connection.affectedRows(
                    """
                    INSERT INTO test (name) VALUES (?)
                  """.trimIndent(),
                    listOf("Name${it}")
                  )
                }
                println("selecting")
                val firstName = connection.values<String>(
                  """
                    SELECT name FROM test
                  """.trimIndent(),
                  "name"
                ).iterate {
                  it.next()
                }
                println(firstName)
                println("mapping")
                println(connection.entries<Int, String>(
                  """
                    SELECT id, name FROM test
                  """.trimIndent(),
                  "id", "name").toMap().
                  map { "${it.key} -> ${it.value}" }.joinToString("\n"))
                delay(1000)

//                println(connection.rows("INSERT INTO test (name) VALUES (?)", listOf("row1")).toList())
//                println(connection.affectedRows("DELETE FROM test WHERE true"))
//                connection.prepare("INSERT INTO test (name) VALUES (?)", "i1").use {
//                  println(it.rows(listOf("row1")).toList())
//                }
//                println(connection.affectedRows("INSERT INTO test (name) VALUES (?)", listOf("row2")))
//                connection.prepare("INSERT INTO test (name) VALUES (?)", "i1").use {
//                  println(it.affectedRows(listOf("row3")))
//                  println(it.affectedRows(listOf("row4")))
//                }
//                println(connection.rows("SELECT * FROM test WHERE id=1234").toList())
//                println(connection.rows("SELECT * FROM test").toList())
              }
            }.join()
          }
        }
        finally {
          Docker.stopContainer(version)
        }
      }
    }
  }

}
