package info.jdavid.postgres

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress

object Authentication {

  internal suspend fun authenticate(connection: PostgresConnection,
                                    credentials: PostgresCredentials): List<Message.FromServer> {
    val messages = connection.receive()
    val authenticationMessage = messages.find { it is Message.Authentication } ?:
                                throw RuntimeException("Expected authentication message.")
    when (authenticationMessage) {
      is Message.AuthenticationOk -> return messages
      is Message.AuthenticationCleartextPassword -> {
        if (credentials !is PostgresCredentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        connection.send(
          Message.PasswordMessage(credentials.username, credentials.password, authenticationMessage)
        )
        return authenticate(connection, credentials)
      }
      is Message.AuthenticationMD5Password -> {
        if (credentials !is PostgresCredentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        connection.send(
          Message.PasswordMessage(credentials.username, credentials.password, authenticationMessage)
        )
        return authenticate(connection, credentials)
      }
      else -> throw Exception("Unsupported authentication method.")
    }
  }

  class Exception(message: String): RuntimeException(message)

  sealed class PostgresCredentials(internal val username: String) {

    class UnsecuredCredentials(username: String = "postgres"): PostgresCredentials(username)
    class PasswordCredentials(username: String = "postgres",
                              internal val password: String = "postgres"): PostgresCredentials(username)

    suspend fun connectTo(
      database: String,
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress (), 5432)
    ): PostgresConnection {
      return PostgresConnection.to(database, this, address)
    }

  }

}
