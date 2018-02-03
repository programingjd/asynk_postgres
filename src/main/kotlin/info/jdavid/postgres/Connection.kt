package info.jdavid.postgres

import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.*
import java.util.concurrent.TimeUnit

class Connection internal constructor(private val channel: AsynchronousSocketChannel,
                                      private val buffer: ByteBuffer): Closeable {
  private val props = mutableMapOf<String, String>()
  private var processId = 0
  private var privateKey = 0
  private var statementCounter = 0

  override fun close() = channel.close()

  fun parameters(): Map<String, String> {
    return props.toMap()
  }

  suspend fun query(sqlStatement: String, vararg params: Any?): Map<String, Any?> {
    val statement = prepare(sqlStatement)
    return query(statement, params)
  }

  suspend fun query(preparedStatement: PreparedStatement, vararg params: Any?): Map<String, Any?> {
    bind(preparedStatement.name, params)
    val messages = receive()
    println(messages.size)
    return emptyMap()
  }

  suspend fun prepare(sqlStatement: String): PreparedStatement {
    val name = "P${++statementCounter}"
    send(Message.Parse(name.toByteArray(Charsets.US_ASCII), sqlStatement))
    send(Message.Sync())
    val messages = receive()
    messages.find { it is Message.ParseComplete } ?: throw exception(messages)
    messages.find { it is Message.ReadyForQuery } ?: throw exception(messages)
    messages.forEach {
      when (it) {
        is Message.ErrorResponse -> throw RuntimeException("Error parsing statement:\n${sqlStatement}\n${it}")
        is Message.NoticeResponse -> warn("Statement:\n${sqlStatement}\n${it}")
      }
    }
    return PreparedStatement(name)
  }

  private suspend fun bind(name: String?, vararg params: Any?) {
    send(Message.Bind(name?.toByteArray(Charsets.US_ASCII), params))
  }

  internal suspend fun send(message: Message.FromClient) {
    message.writeTo(buffer.clear())
    channel.aWrite(buffer.flip(), 5000L, TimeUnit.MILLISECONDS)
  }

  internal suspend fun receive(): List<Message.FromServer> {
    buffer.clear()
    channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
    buffer.flip()
    val list = LinkedList<Message.FromServer>()
    while(buffer.remaining() > 0) {
      val message = Message.fromBytes(buffer)
      list.add(message)
    }
    return list
  }

  class PreparedStatement internal constructor(internal val name: String)

  companion object {
    suspend fun to(
      database: String,
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 5432),
      credentials: Authentication.Credentials = Authentication.Credentials.UnsecuredCredentials()
    ): Connection {
      val channel = AsynchronousSocketChannel.open()
      try {
        channel.aConnect(address)
        val buffer = ByteBuffer.allocate(4096)
        val connection = Connection(channel, buffer)
        connection.send(Message.StartupMessage(credentials.username, database))
        val messages = Authentication.authenticate(connection, credentials)
        messages.find { it is Message.ReadyForQuery } ?: throw exception(messages)
        val (processId, privateKey) =
          (messages.find { it is Message.BackendKeyData } ?: throw exception(messages))
            as Message.BackendKeyData
        connection.processId = processId
        connection.privateKey = privateKey
        messages.forEach {
          if (it is Message.ParameterStatus) connection.props[it.key] = it.value
        }
        return connection
      }
      catch (e: Exception) {
        channel.close()
        throw e
      }
    }
    private fun exception(messages: List<Message.FromServer>): Exception {
      return messages.find { it is Message.ErrorResponse }?.
        let { RuntimeException(it.toString()) } ?: RuntimeException()
    }
    private val logger = LoggerFactory.getLogger(Connection::class.java)
    private fun warn(message: String) = logger.warn(message)
  }

}
