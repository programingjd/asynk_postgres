package info.jdavid.asynk.postgres

import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
typealias PostgresCredentials=info.jdavid.asynk.sql.Credentials<PostgresConnection>

object PostgresAuthentication {

  internal suspend fun authenticate(connection: PostgresConnection,
                                    credentials: PostgresCredentials) {
    val message = connection.message()
    if (message is Message.ErrorResponse) throw RuntimeException(message.toString())
    if (message !is Message.Authentication) throw RuntimeException("Expected authentication message.")
    when (message) {
      is Message.AuthenticationOk -> Unit
      is Message.AuthenticationCleartextPassword -> {
        if (credentials !is Credentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        connection.send(
          Message.PasswordMessage(credentials.username, credentials.password, message)
        )
        authenticate(connection, credentials)
      }
      is Message.AuthenticationMD5Password -> {
        if (credentials !is Credentials.PasswordCredentials) throw Exception("Incompatible credentials.")
        connection.send(
          Message.PasswordMessage(credentials.username, credentials.password, message)
        )
        authenticate(connection, credentials)
      }
      else -> throw Exception("Unsupported authentication method.")
    }
  }

  class Exception(message: String): RuntimeException(message)

  /**
   * Implementations of the different credentials available for PostgreSQL.
   */
  sealed class Credentials(internal val username: String): PostgresCredentials {

    /**
     * Username only (no password) unsecured credentials.
     * @param username the database username (postgres by default).
     */
    class UnsecuredCredentials(username: String = "postgres"): Credentials(username)

    /**
     * Username/password credentials.
     * @param username the database username (postgres by default).
     * @param password the user password (postgres by default).
     */
    class PasswordCredentials(username: String = "postgres",
                              internal val password: String = "postgres"): Credentials(username)

    override suspend fun connectTo(database: String) = connectTo(
      database, InetSocketAddress(InetAddress.getLoopbackAddress(), 5432), 4194304
    )
    override suspend fun connectTo(database: String, bufferSize: Int) = connectTo(
      database, InetSocketAddress(InetAddress.getLoopbackAddress(), 5432), bufferSize
    )

    override suspend fun connectTo(database: String, address: InetAddress) = connectTo(
      database, InetSocketAddress(address, 5432), 4194304
    )
    override suspend fun connectTo(database: String, address: InetAddress, bufferSize: Int) = connectTo(
      database, InetSocketAddress(address, 5432), bufferSize
    )

    override suspend fun connectTo(database: String, address: SocketAddress) = connectTo(
      database, address, 4194304
    )
    override suspend fun connectTo(database: String, address: SocketAddress,
                                   bufferSize: Int): PostgresConnection {
      return PostgresConnection.to(database, this, address, bufferSize)
    }

  }

}
