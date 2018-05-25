package info.jdavid.asynk.postgres

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import info.jdavid.asynk.sql.use
import kotlinx.coroutines.experimental.runBlocking

fun json(any: Any?) = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(any)

fun main(args: Array<String>) {
  val properties = Utils.properties()
  val username = properties.getProperty("postgres_username") ?: "postgres"
  val password = properties.getProperty("postgres_password") ?: "postgres"
  val credentials = PostgresAuthentication.Credentials.PasswordCredentials(username, password)
  val database = properties.getProperty("postgres_database") ?: "postgres"
  runBlocking {
    credentials.connectTo(database).use {
      println(it.parameters())
      //val prepared = it.prepare("SELECT * FROM test")
      //it.close(prepared)
      //it.rows("SELECT * FROM test")
      //delay(5000)
      for (row in it.rows("SELECT * FROM test")) {
        println(json(row))
      }
//      it.rows("SELECT * FROM test").apply {
////        val iterator = iterator()
////        println(json(iterator.next()))
////        println(json(iterator.next()))
//        //println(json(iterator.next()))
//      }
//      it.rows("SELECT * FROM test")
//      delay(5000)
      //println(it.affectedRows("INSERT INTO test (name) VALUES (?);", listOf("Name2")))
      //for (i in 1..500) {
      //  println(it.affectedRows("INSERT INTO test (name) VALUES (?);", listOf("Name${i+502}")))
      //}
    }
  }
}

