package info.jdavid.asynk.postgres

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.net.InetAddress
import java.net.InetSocketAddress

class Debug {

  companion object {
    @JvmStatic
    fun main(args: Array<String>) {
      val credentials =
        PostgresAuthentication.Credentials.PasswordCredentials("test", "asynk")
      val databaseName = "world"
      Docker.DatabaseVersion.values().last().let { version ->
        Docker.startContainer(version)
        try {
          runBlocking {
            launch {
              val address = InetSocketAddress(InetAddress.getLocalHost(), version.port)
              credentials.connectTo(databaseName, address).use { connection ->
                println("connected")
                println(connection.affectedRows(
                  """
                    CREATE TEMPORARY TABLE test (
                      id             serial    PRIMARY KEY,
                      name           text      NOT NULL,
                      active         boolean   DEFAULT FALSE NOT NULL,
                      creation_date  date      DEFAULT current_timestamp
                    )
                  """.trimIndent()
                ))
                println(connection.rows("INSERT INTO test (name) VALUES (?)", listOf("row1")).toList())
                println(connection.affectedRows("DELETE FROM test WHERE true"))
                connection.prepare("INSERT INTO test (name) VALUES (?)", "i1").use {
                  println(it.rows(listOf("row1")).toList())
                }
                println(connection.affectedRows("INSERT INTO test (name) VALUES (?)", listOf("row2")))
                connection.prepare("INSERT INTO test (name) VALUES (?)", "i1").use {
                  println(it.affectedRows(listOf("row3")))
                  println(it.affectedRows(listOf("row4")))
                }
                println(connection.rows("SELECT * FROM test WHERE id=1234").toList())
                println(connection.rows("SELECT * FROM test").toList())
              }
            }
          }
        }
        finally {
          Docker.stopContainer(version)
        }
      }
    }
  }

}
