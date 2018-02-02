package info.jdavid.postgres

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress

object Authentication {

  internal suspend fun authenticate(connection: Connection,
                                    credentials: Credentials): List<Message.FromServer> {
    val messages = connection.receive()
    val authenticationMessage = messages.find { it is Message.Authentication } ?:
                                throw RuntimeException("Expected authentication message.")
    when (authenticationMessage) {
      is Message.AuthenticationOk -> return messages
      is Message.AuthenticationCleartextPassword -> {
        if (credentials !is Credentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        connection.send(
          Message.PasswordMessage(credentials.username, credentials.password, authenticationMessage)
        )
        return authenticate(connection, credentials)
      }
      is Message.AuthenticationMD5Password -> {
        if (credentials !is Credentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        connection.send(
          Message.PasswordMessage(credentials.username, credentials.password, authenticationMessage)
        )
        return authenticate(connection, credentials)
      }
      else -> throw Exception("Unsupported authentication method.")
    }
  }

  class Exception(message: String): RuntimeException(message)

  sealed class Credentials(internal val username: String) {

    class UnsecuredCredentials(username: String = "postgres"): Credentials(username)
    class PasswordCredentials(username: String, internal val password: String): Credentials(username)

    suspend fun connectTo(
      database: String,
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress (), 5432)
    ): Connection {
      return Connection.to(database, address,this)
    }

  }

}
