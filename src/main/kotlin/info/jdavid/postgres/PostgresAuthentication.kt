package info.jdavid.postgres

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
typealias PostgresCredentials=info.jdavid.sql.Credentials<PostgresConnection>

object PostgresAuthentication {

  internal suspend fun authenticate(connection: PostgresConnection,
                                    credentials: PostgresCredentials): List<Message.FromServer> {
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

  sealed class Credentials(internal val username: String): PostgresCredentials {

    class UnsecuredCredentials(username: String = "postgres"): Credentials(username)
    class PasswordCredentials(username: String = "postgres",
                              internal val password: String = "postgres"): Credentials(username)

    override suspend fun connectTo(database: String) = connectTo(
      database, InetSocketAddress(InetAddress.getLoopbackAddress(), 5432)
    )

    override suspend fun connectTo(database: String, address: SocketAddress): PostgresConnection {
      return PostgresConnection.to(database, this, address)
    }

  }

}
