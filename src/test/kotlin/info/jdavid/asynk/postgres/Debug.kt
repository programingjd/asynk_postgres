package info.jdavid.asynk.postgres

import info.jdavid.asynk.sql.use
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withContext
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
            withContext(CommonPool) {
              val address = InetSocketAddress(InetAddress.getLocalHost(), version.port)
              credentials.connectTo(databaseName, address).use {
                println("connected")
                println(it.affectedRows(
                  """
                    CREATE TEMPORARY TABLE test (
                      id             serial    PRIMARY KEY,
                      name           text      NOT NULL,
                      active         boolean   DEFAULT FALSE NOT NULL,
                      creation_date  date      DEFAULT current_timestamp
                    )
                  """.trimIndent()
                ))
                println(it.rows("INSERT INTO test (name) VALUES (?)", listOf("row1")).toList())
                println(it.affectedRows("DELETE FROM test WHERE true"))
                it.prepare("INSERT INTO test (name) VALUES (?)", "i1").use {
                  println(it.rows(listOf("row1")).toList())
                }
                println(it.affectedRows("INSERT INTO test (name) VALUES (?)", listOf("row2")))
                it.prepare("INSERT INTO test (name) VALUES (?)", "i1").use {
                  println(it.affectedRows(listOf("row3")))
                  println(it.affectedRows(listOf("row4")))
                }
                println(it.rows("SELECT * FROM test WHERE id=1234").toList())
                println(it.rows("SELECT * FROM test").toList())
              }
            }
          }
        }
        finally {
//          Docker.stopContainer(version)
        }
      }
    }
  }

}
