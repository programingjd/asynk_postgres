package info.jdavid.asynk.postgres

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClients
import java.io.File
import java.net.URLEncoder

object Docker {

  private fun dataPath(): File {
    var dir: File? = File(this::class.java.protectionDomain.codeSource.location.toURI())
    while (dir != null) {
      if (File(dir, ".git").exists()) break
      dir = dir.parentFile
    }
    dir = File(dir, "data")
    return dir.canonicalFile
  }

  private val dockerApiUrl = "http://localhost:2375"

  enum class DatabaseVersion(val label: String, val port: Int) {
    POSTGRES_93("library/postgres:9.3", 5433),
    POSTGRES_94("library/postgres:9.4", 5434),
    POSTGRES_95("library/postgres:9.5", 5435),
    POSTGRES_96("library/postgres:9.6", 5436),
    POSTGRES_105("library/postgres:10.5", 5437)
  }

  fun check() {
    HttpClients.createMinimal().let {
      try {
        it.execute(HttpGet("${dockerApiUrl}/version")).use {
          if (it.statusLine.statusCode != 200) {
            throw RuntimeException("Docker is unreachable.")
          }
        }
      }
      catch (e: Exception) {
        println(
          "Docker did not respond. Please make sure that docker is running and that the option to expose " +
            "the daemon on tcp without TLS is enabled in the settings."
        )
        e.printStackTrace()
        throw e
      }
    }
  }

  fun startContainer(databaseVersion: DatabaseVersion) {
    HttpClients.createMinimal().let {
      it.execute(HttpPost(
        "${dockerApiUrl}/images/create?fromImage=${databaseVersion.label}"
      )).use {
        println(String(it.entity.content.readAllBytes()))
      }
      it.execute(HttpPost(
        "${dockerApiUrl}/containers/create?name=async_${databaseVersion.name.toLowerCase()}"
      ).apply {
        val body = mapOf(
          "Image" to databaseVersion.label,
          "Env" to listOf(
            "POSTGRES_DB=world",
            "POSTGRES_USER=test",
            "POSTGRES_PASSWORD=asynk"
          ),
          "Healthcheck" to mapOf(
            "Test" to listOf("CMD-SHELL", "pg_isready -U postgres"),
            "Interval" to 10000000000,
            "Timeout" to 5000000000,
            "Retries" to 5,
            "StartPeriod" to 0
          ),
          "HostConfig" to mapOf(
            "Binds" to listOf("${dataPath().path.replace('\\','/')}:/docker-entrypoint-initdb.d/"),
            "PortBindings" to mapOf(
              "5432/tcp" to listOf(
                mapOf("HostPort" to "${databaseVersion.port}")
              )
            )
          ),
          "ExposedPorts" to mapOf("5432/tcp" to emptyMap<String, Any>())
        )
        entity = ByteArrayEntity(ObjectMapper().writeValueAsBytes(body), ContentType.APPLICATION_JSON)
      }).use {
        println(String(it.entity.content.readAllBytes()))
      }

      it.execute(HttpPost(
        "${dockerApiUrl}/containers/async_${databaseVersion.name.toLowerCase()}/start"
      )).use {
        if (it.statusLine.statusCode != 204 && it.statusLine.statusCode != 304)
          throw RuntimeException(String(it.entity.content.readAllBytes()))
      }

      val id = it.execute(HttpGet(
        "${dockerApiUrl}/containers/json?name=async_${databaseVersion.name.toLowerCase()}"
      )).use {
        val list = ObjectMapper().readValue(it.entity.content.readAllBytes(), ArrayList::class.java)
        if (list.isEmpty()) throw RuntimeException("Failed to create container.")
        @Suppress("UNCHECKED_CAST")
        (list[0] as Map<String, Any?>)["Id"] as String
      }

      print("Waiting for container to start.")
      var counter = 0
      val filters = "{\"id\":[\"${id}\"],\"health\":[\"healthy\",\"none\"]}"
      while (true) {
        val list = it.execute(HttpGet(
          "${dockerApiUrl}/containers/json?filters=${URLEncoder.encode(filters,"UTF-8")}"
        )).use {
          ObjectMapper().readValue(it.entity.content.readAllBytes(), ArrayList::class.java)
        }
//        val found = list.find {
//          @Suppress("UNCHECKED_CAST")
//          (list[0] as Map<String, Any?>)["Id"] == id
//        }
//        if (found != null) break
        if (list.size > 0) break
        if (++counter < 90) {
          Thread.sleep(2000)
          print(".")
        }
        else throw RuntimeException("Failed to start container.")
      }
      println()
    }
  }

  fun stopContainer(databaseVersion: DatabaseVersion) {
    HttpClients.createMinimal().let {
      it.execute(HttpPost(
        "${dockerApiUrl}/containers/async_${databaseVersion.name.toLowerCase()}/stop"
      )).use {
        if (it.statusLine.statusCode != 204 && it.statusLine.statusCode != 304)
          throw RuntimeException(String(it.entity.content.readAllBytes()))
      }
    }
    HttpClients.createMinimal().let {
      it.execute(HttpDelete(
        "${dockerApiUrl}/containers/async_${databaseVersion.name.toLowerCase()}"
      )).use {
        if (it.statusLine.statusCode != 204 && it.statusLine.statusCode != 404)
          throw RuntimeException(String(it.entity.content.readAllBytes()))
      }
    }
  }

  fun createWorldDatabase(databaseVersion: DatabaseVersion) {
    HttpClients.createMinimal().let {
      val id = it.execute(HttpPost(
        "${dockerApiUrl}/containers/async_${databaseVersion.name.toLowerCase()}/exec"
      ).apply {
        val body = mapOf(
          "Cmd" to listOf(
            "mysql -uroot -proot < ${File(dataPath(), "world.sql").path}"
          )
        )
        entity = ByteArrayEntity(ObjectMapper().writeValueAsBytes(body), ContentType.APPLICATION_JSON)
      }).use {
        ObjectMapper().readTree(it.entity.content.readAllBytes()).findValue("Id").asText()
      }
      it.execute(HttpPost(
        "${dockerApiUrl}/exec/${id}/start"
      )).use {
        println(String(it.entity.content.readAllBytes()))
      }
      it.execute(HttpGet(
        "${dockerApiUrl}/exec/${id}/json"
      )).use {
        println(String(it.entity.content.readAllBytes()))
      }
    }
  }

  @JvmStatic
  fun main(args: Array<String>) {
    check()
    DatabaseVersion.values().last().let { version ->
      startContainer(version)
      try {
        createWorldDatabase(version)
      }
      finally {
        stopContainer(version)
      }
    }
  }

}
