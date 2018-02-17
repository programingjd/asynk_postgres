package info.jdavid.postgres

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking

fun json(any: Any?) = ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValueAsString(any)

fun main(args: Array<String>) {
  val username = "postgres"
  val password = "postgres"
  val database = "postgres"
  runBlocking {
    Authentication.Credentials.PasswordCredentials(username, password).
      connectTo(database).use {
      println(it.parameters())
      //val prepared = it.prepare("SELECT * FROM test")
      //it.close(prepared)
//      it.rows("SELECT * FROM test")
//      delay(5000)
//      for (row in it.rows("SELECT * FROM test")) {
//        println(json(row))
//      }
      it.rows("SELECT * FROM test").apply {
        val iterator = iterator()
        println(json(iterator.next()))
        println(json(iterator.next()))
        //println(json(iterator.next()))
      }
      it.rows("SELECT * FROM test")
      delay(5000)
      //println(it.affectedRows("INSERT INTO test (name) VALUES (?);", listOf("Name2")))
      //for (i in 1..500) {
      //  println(it.affectedRows("INSERT INTO test (name) VALUES (?);", listOf("Name${i+502}")))
      //}
    }
  }
}

