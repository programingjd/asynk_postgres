package info.jdavid.postgres

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
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
      val prepared = it.prepare("SELECT * FROM test")
      it.close(prepared)
      println(json(it.query("SELECT * FROM test")))
      println(it.update("INSERT INTO test (name) VALUES (?);", listOf("Name2")))
    }
  }
}

