package info.jdavid.postgres

import info.jdavid.sql.Connection
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.toList
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.LinkedList
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.EmptyCoroutineContext
import kotlin.coroutines.experimental.coroutineContext

typealias PreparedStatement=Connection.PreparedStatement<PostgresConnection>

class PostgresConnection internal constructor(
                         private val channel: AsynchronousSocketChannel,
                         private val buffer: ByteBuffer): Connection<PostgresConnection> {
  private val props = mutableMapOf<String, String>()
  private var processId = 0
  private var privateKey = 0
  private var statementCounter = 0

  override suspend fun aClose() {
    try {
      send(Message.Terminate())
    }
    finally {
      channel.close()
    }
  }

  fun parameters(): Map<String, String> {
    return props.toMap()
  }

  override suspend fun affectedRows(sqlStatement: String) = affectedRows(sqlStatement, emptyList())

  override suspend fun affectedRows(sqlStatement: String, params: Iterable<Any?>): Int {
    val statement = prepare(sqlStatement, null)
    return affectedRows(statement, params)
  }

  override suspend fun affectedRows(
    preparedStatement: PreparedStatement
  ) = affectedRows(preparedStatement, emptyList())

  override suspend fun affectedRows(preparedStatement: PreparedStatement,
                                    params: Iterable<Any?>): Int {
    if (preparedStatement !is PostgresPreparedStatement) throw IllegalArgumentException()
    val portalName: ByteArray? = null
    val unnamed = preparedStatement.name == null
    bind(preparedStatement.name, portalName, params)
    describe(portalName)
    execute(portalName, 0)
    if (unnamed) close(preparedStatement) else close(portalName)
    sync()
    val messages = receive()
    if (unnamed) {
      messages.find { it is Message.ParseComplete } ?: throw RuntimeException(
        "Failed to parse statement:\n${preparedStatement.query}")
    }
    messages.find { it is Message.BindComplete } ?: throw RuntimeException(
      "Failed to bind parameters to statement:\n${preparedStatement.query}\n${params.joinToString(", ")}"
    )
    val tag =
      (
        (messages.find { it is Message.CommandComplete } ?: throw RuntimeException())
          as Message.CommandComplete
      ).tag
    val i = tag.indexOf(' ').apply { if (this == -1) throw RuntimeException() }
    val command = tag.substring(0, i)
    return when (command) {
      "INSERT" -> {
        val j = tag.indexOf(' ', i + 1).apply { if (this == -1) throw RuntimeException() }
        tag.substring(j + 1).toInt()
      }
      "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY" -> tag.substring(i + 1).toInt()
      else -> 0
    }
  }

  override suspend fun rows(sqlStatement: String) = rows(sqlStatement, emptyList())

  override suspend fun rows(sqlStatement: String, params: Iterable<Any?>): PostgresResultSet {
    val statement = prepare(sqlStatement, null)
    return rows(statement, params)
  }

  override suspend fun rows(preparedStatement: PreparedStatement) = rows(preparedStatement, emptyList())

  override suspend fun rows(preparedStatement: PreparedStatement,
                            params: Iterable<Any?>): PostgresResultSet {
    if (preparedStatement !is PostgresPreparedStatement) throw IllegalArgumentException()
    val batchSize = 100
    val portalName: ByteArray? = null
    val unnamed = preparedStatement.name == null
    bind(preparedStatement.name, portalName, params)
    describe(portalName)
    execute(portalName, batchSize)
    send(Message.Flush())
    val messages = receive()
    if (unnamed) {
      messages.find { it is Message.ParseComplete } ?: throw RuntimeException(
        "Failed to parse statement:\n${preparedStatement.query}")
    }
    messages.find { it is Message.BindComplete } ?: throw RuntimeException(
      "Failed to bind parameters to statement:\n${preparedStatement.query}\n${params.joinToString(", ")}"
    )
    val fields =
      messages.find { it is Message.NoData || it is Message.RowDescription }?.
        let { if (it is Message.RowDescription) it.fields else emptyList() } ?:
      throw RuntimeException()

    val channel = Channel<Map<String, Any?>>(batchSize)
    launch(coroutineContext + EmptyCoroutineContext) {
      var m = messages
      while (true) {
        // TODO check if close for send and cancel portal is so
        (appendResults(fields, m, batchSize)).forEach { channel.send(it) }
        if (m.find { it is Message.CommandComplete } != null) {
          break
        }
        m.find { it is Message.PortalSuspended } ?: throw RuntimeException()
        execute(portalName, batchSize)
        send(Message.Flush())
        m = receive()
      }
      if (unnamed) close(preparedStatement) else close(portalName)
      sync()
      m = receive()
      m.find { it is Message.CloseComplete } ?: throw RuntimeException()
      m.find { it is Message.ReadyForQuery } ?: throw RuntimeException()
      channel.close()
    }
    return PostgresResultSet(channel)
  }

  private fun appendResults(fields: List<Pair<String, String>>,
                            messages: List<Message.FromServer>,
                            batchSize: Int): List<Map<String, Any?>> {
    val list = ArrayList<Map<String, Any?>>(batchSize)
    val n = fields.size
    messages.filterIsInstance<Message.DataRow>().forEach {
      val values = it.values
      assert(n == values.size)
      val map = LinkedHashMap<String, Any?>(n)
      for (i in 0 until n) {
        val field = fields[i]
        val result = values[i]
        map[field.first] = if (result == null) null else TextFormat.parse(field.second, result)
      }
      list.add(map)
    }
    return list
  }

  suspend fun close(preparedStatement: PostgresPreparedStatement) {
    send(Message.ClosePreparedStatement(preparedStatement.name))
    if (preparedStatement.name != null) {
      send(Message.Flush())
      val messages = receive()
      messages.find { it is Message.CloseComplete } ?: throw RuntimeException()
    }
  }

  override suspend fun prepare(sqlStatement: String) = prepare(sqlStatement, "__ps_${++statementCounter}")

  suspend fun prepare(sqlStatement: String,
                      name: String = "__ps_${++statementCounter}"): PostgresPreparedStatement {
    return prepare(sqlStatement, name.toByteArray(Charsets.US_ASCII))
  }

  private suspend fun prepare(sqlStatement: String, name: ByteArray?): PostgresPreparedStatement {
    val preparedStatement = PostgresPreparedStatement(name, sqlStatement)
    send(Message.Parse(name, preparedStatement.query))
    if (name != null) {
      send(Message.Flush())
      val messages = receive()
      messages.find { it is Message.ParseComplete } ?:
        throw RuntimeException("Failed to parse statement:\n${sqlStatement}")
    }
    return preparedStatement
  }

  private suspend fun bind(preparedStatementName: ByteArray?,
                           portalName: ByteArray?,
                           params: Iterable<Any?>) {
    send(Message.Bind(preparedStatementName, portalName, params))
  }

  private suspend fun describe(portalName: ByteArray?) {
    send(Message.Describe(portalName))
  }

  private suspend fun execute(portalName: ByteArray?, maxRows: Int) {
    send(Message.Execute(portalName, maxRows))
  }

  private suspend fun close(portalName: ByteArray?) {
    send(Message.ClosePortal(portalName))
  }

  private suspend fun sync() {
    send(Message.Sync())
  }

  internal suspend fun send(message: Message.FromClient) {
    message.writeTo(buffer.clear() as ByteBuffer)
    channel.aWrite(buffer.flip() as ByteBuffer, 5000L, TimeUnit.MILLISECONDS)
  }

  internal suspend fun receive(): List<Message.FromServer> {
    buffer.clear()
    val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
    if (n == buffer.capacity()) throw RuntimeException("Connection buffer too small.")
    buffer.flip()
    val list = LinkedList<Message.FromServer>()
    while(buffer.remaining() > 0) {
      val message = Message.fromBytes(buffer)
      list.add(message)
    }
    list.forEach {
      when (it) {
        is Message.ErrorResponse -> err(it.toString())
        is Message.NoticeResponse -> warn(it.toString())
        is Message.ParameterStatus -> props[it.key] = it.value
      }
    }
    return list
  }

  inner class PostgresPreparedStatement internal constructor(
                                        internal val name: ByteArray?,
                                        query: String) : PreparedStatement {
    internal val query: String
    init {
      val sb = StringBuilder(query.length + 16)
      var start = 0
      var i = 0
      while (true) {
        val index = query.indexOf('?', start)
        if (index == -1) {
          sb.append(query.substring(start))
          break
        }
        sb.append(query.substring(start, index))
        sb.append("\$${++i}")
        start = index + 1
      }
      this.query = sb.toString()
    }
    override suspend fun rows() = this@PostgresConnection.rows(this)
    override suspend fun rows(params: Iterable<Any?>) = this@PostgresConnection.rows(this, params)
    override suspend fun affectedRows() = this@PostgresConnection.affectedRows(this)
    override suspend fun affectedRows(
      params: Iterable<Any?>
    ) = this@PostgresConnection.affectedRows(this, params)
    override suspend fun aClose() = this@PostgresConnection.close(this)
  }

  class PostgresResultSet internal constructor(
                          private val channel: Channel<Map<String, Any?>>): Connection.ResultSet {
    override operator fun iterator() = channel.iterator()
    override fun close() { channel.cancel() }
    override suspend fun toList() = channel.toList()
  }

  companion object {
    suspend fun to(
      database: String,
      credentials: PostgresAuthentication.Credentials =
        PostgresAuthentication.Credentials.UnsecuredCredentials(),
      address: SocketAddress = InetSocketAddress(InetAddress.getLoopbackAddress(), 5432),
      bufferSize: Int = 4194304 // needs to hold any RowData message
    ): PostgresConnection {
      val channel = AsynchronousSocketChannel.open()
      try {
        if (bufferSize < 1024) throw IllegalArgumentException(
          "Buffer size ${bufferSize} is smaller than the minumum buffer size 1024."
        )
        channel.aConnect(address)
        val buffer = ByteBuffer.allocateDirect(bufferSize)
        val connection = PostgresConnection(channel, buffer)
        connection.send(Message.StartupMessage(credentials.username, database))
        val messages = PostgresAuthentication.authenticate(connection, credentials)
        messages.find { it is Message.ReadyForQuery } ?: throw RuntimeException()
        val (processId, privateKey) =
          (messages.find { it is Message.BackendKeyData } ?: throw RuntimeException())
            as Message.BackendKeyData
        connection.processId = processId
        connection.privateKey = privateKey
        return connection
      }
      catch (e: Exception) {
        channel.close()
        throw e
      }
    }
    private val logger = LoggerFactory.getLogger(PostgresConnection::class.java)
    private fun warn(message: String) = logger.warn(message)
    private fun err(message: String) = logger.error(message)
  }

}
