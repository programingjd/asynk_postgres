package info.jdavid.asynk.postgres

import info.jdavid.asynk.sql.Connection
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
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.EmptyCoroutineContext
import kotlin.coroutines.experimental.coroutineContext

typealias PreparedStatement=Connection.PreparedStatement<PostgresConnection>

/**
 * PostgreSQL database connection.
 */
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


  override suspend fun startTransaction() {
    affectedRows("BEGIN")
  }

  override suspend fun commitTransaction() {
    affectedRows("COMMIT")
  }

  override suspend fun rollbackTransaction() {
    affectedRows("ROLLBACK")
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
    if (unnamed) {
      message().apply {
        when (this) {
          is Message.ErrorResponse -> throw RuntimeException(
            "Failed to parse statement:\n${preparedStatement.query}\n${this}"
          )
          is Message.ParseComplete -> Unit
          else -> throw RuntimeException(
            "Failed to parse statement:\n${preparedStatement.query}"
          )
        }
      }
    }
    message().apply {
      when (this) {
        is Message.ErrorResponse ->  throw RuntimeException(
          "Failed to bind parameters to statement:\n" +
          "${preparedStatement.query}\n${params.joinToString(", ")}\n${this}"
        )
        is Message.BindComplete -> Unit
        else -> throw RuntimeException(
          "Failed to bind parameters to statement:\n" +
          "${preparedStatement.query}\n${params.joinToString(", ")}"
        )
      }
    }
    val tag = message().let {
      when (it) {
        is Message.ErrorResponse -> throw RuntimeException(
          "Failed to execute statement:\n${preparedStatement.query}\n${it}"
        )
        is Message.NoData -> message().let { m ->
          when (m) {
            is Message.ErrorResponse -> throw RuntimeException(
              "Failed to execute statement:\n${preparedStatement.query}\n${m}"
            )
            is Message.CommandComplete -> m.tag
            else -> throw RuntimeException(
              "Failed to execute statement:\n${preparedStatement.query}"
            )
          }
        }
        is Message.CommandComplete -> it.tag
        else -> throw RuntimeException(
          "Failed to execute statement:\n${preparedStatement.query}"
        )
      }
    }
    message().apply {
      when (this) {
        is Message.ErrorResponse -> throw RuntimeException(
          if (unnamed) {
            "Failed to close statement:\n${preparedStatement.name}\n${this}"
          }
          else {
            "Failed to close portal.\n${this}"
          }
        )
        is Message.CloseComplete -> Unit
        else -> throw RuntimeException(
          if (unnamed) {
            "Failed to close statement:\n${preparedStatement.name}\n"
          }
          else {
            "Failed to close portal.\n${this}"
          }
        )
      }
    }
    message().apply {
      when (this) {
        is Message.ErrorResponse -> throw RuntimeException(this.toString())
        is Message.ReadyForQuery -> Unit
        else -> throw RuntimeException()
      }
    }

    val i = tag.indexOf(' ')
    val command = if (i == -1) tag else tag.substring(0, i)
    return when (command) {
      "INSERT" -> {
        if (i == -1) throw RuntimeException()
        val j = tag.indexOf(' ', i + 1).apply { if (this == -1) throw RuntimeException() }
        tag.substring(j + 1).toInt()
      }
      "DELETE", "UPDATE", "SELECT", "MOVE", "FETCH", "COPY" -> {
        if (i == -1) throw RuntimeException()
        tag.substring(i + 1).toInt()
      }
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
    if (unnamed) {
      message().apply {
        when (this) {
          is Message.ErrorResponse -> throw RuntimeException(
            "Failed to parse statement:\n${preparedStatement.query}\n${this}"
          )
          is Message.ParseComplete -> Unit
          else -> throw RuntimeException(
            "Failed to parse statement:\n${preparedStatement.query}"
          )
        }
      }
    }
    message().apply {
      when (this) {
        is Message.ErrorResponse ->  throw RuntimeException(
          "Failed to bind parameters to statement:\n" +
          "${preparedStatement.query}\n${params.joinToString(", ")}\n${this}"
        )
        is Message.BindComplete -> Unit
        else -> throw RuntimeException(
          "Failed to bind parameters to statement:\n" +
          "${preparedStatement.query}\n${params.joinToString(", ")}"
        )
      }
    }
    val fields = message().let {
      when (it) {
        is Message.ErrorResponse -> throw RuntimeException(
          "Failed to execute statement:\n${preparedStatement.query}\n${it}"
        )
        is Message.NoData -> {
          message().apply {
            when (this) {
              is Message.ErrorResponse -> throw RuntimeException(
                "Failed to execute statement:\n${preparedStatement.query}\n${this}"
              )
              is Message.CommandComplete -> Unit
              else -> throw RuntimeException(
                "Failed to execute statement:\n${preparedStatement.query}"
              )
            }
          }
          if (unnamed) close(preparedStatement) else close(portalName)
          sync()
          message().apply {
            when (this) {
              is Message.ErrorResponse -> throw RuntimeException(
                if (unnamed) {
                  "Failed to close statement:\n${preparedStatement.name}\n${this}"
                }
                else {
                  "Failed to close portal.\n${this}"
                }
              )
              is Message.CloseComplete -> Unit
              else -> throw RuntimeException(
                if (unnamed) {
                  "Failed to close statement:\n${preparedStatement.name}\n"
                }
                else {
                  "Failed to close portal.\n${this}"
                }
              )
            }
          }
          message().apply {
            when (this) {
              is Message.ErrorResponse -> throw RuntimeException(this.toString())
              is Message.ReadyForQuery -> Unit
              else -> throw RuntimeException()
            }
          }
          val channel = Channel<Map<String, Any?>>()
          channel.close()
          return PostgresResultSet(channel)
        }
        is Message.RowDescription -> it.fields
        else -> throw RuntimeException(
          "Failed to execute statement:\n${preparedStatement.query}"
        )
      }
    }
    val channel = Channel<Map<String, Any?>>(batchSize)
    launch(coroutineContext + EmptyCoroutineContext) {
      loop@ while (true) {
        val n = fields.size
        val message = message()
        when (message) {
          is Message.ErrorResponse -> throw RuntimeException("Failed to fetch row.\n${this}")
          is Message.DataRow -> {
            val values = message.values
            assert(n == values.size)
            val map = LinkedHashMap<String, Any?>(n)
            for (i in 0 until n) {
              val field = fields[i]
              val result = values[i]
              map[field.first] = if (result == null) null else TextFormat.parse(field.second, result)
            }
            channel.send(map)
          }
          is Message.PortalSuspended -> {
            execute(portalName, batchSize)
            send(Message.Flush())
          }
          is Message.CommandComplete -> break@loop
        }
      }
      if (unnamed) close(preparedStatement) else close(portalName)
      sync()
      message().apply {
        when (this) {
          is Message.ErrorResponse -> throw RuntimeException(
            if (unnamed) {
              "Failed to close statement:\n${preparedStatement.name}\n${this}"
            }
            else {
              "Failed to close portal.\n${this}"
            }
          )
          is Message.CloseComplete -> Unit
          else -> throw RuntimeException(
            if (unnamed) {
              "Failed to close statement:\n${preparedStatement.name}\n"
            }
            else {
              "Failed to close portal.\n${this}"
            }
          )
        }
      }
      message().apply {
        when (this) {
          is Message.ErrorResponse -> throw RuntimeException(this.toString())
          is Message.ReadyForQuery -> Unit
          else -> throw RuntimeException()
        }
      }
      channel.close()
    }
    return PostgresResultSet(channel)
  }

  suspend fun close(preparedStatement: PostgresPreparedStatement) {
    send(Message.ClosePreparedStatement(preparedStatement.name))
    if (preparedStatement.name != null) {
      sync()
      message().apply {
        when (this) {
          is Message.ErrorResponse -> throw RuntimeException(
            "Failed to close statement ${preparedStatement.name}."
          )
          is Message.CloseComplete -> {
            message().apply {
              when (this) {
                is Message.ErrorResponse -> throw RuntimeException(
                  "Failed to close statement ${preparedStatement.name}."
                )
                is Message.ReadyForQuery -> Unit
                else -> throw RuntimeException(
                  "Failed to close statement ${preparedStatement.name}."
                )
              }
            }
          }
          is Message.ReadyForQuery -> Unit
        }
      }
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
      loop@ while (true) {
        val message = message()
        when (message) {
          is Message.ErrorResponse -> throw RuntimeException(
            "Failed to parse statement:\n${preparedStatement.query}\n${message}"
          )
          is Message.ParseComplete -> break@loop
          is Message.ReadyForQuery -> Unit
          else -> throw RuntimeException(
            "Failed to parse statement:\n${preparedStatement.query}\n${message}"
          )
        }
      }
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

  private suspend fun query(query: String) {
    send(Message.Query(query))
  }

  internal suspend fun send(message: Message.FromClient) {
    message.writeTo(buffer.clear() as ByteBuffer)
    (buffer.flip() as ByteBuffer).apply {
      while (remaining() > 0) channel.aWrite(this, 5000L, TimeUnit.MILLISECONDS)
    }
  }

  private suspend fun anyMessage(): Message.FromServer? {
    if (buffer.remaining() > 0) {
      if (buffer.remaining() > 4) {
        val message = Message.fromBytes(buffer)
        if (message != null) return message
      }
    }
    while (true) {
      buffer.compact()
      val left = buffer.capacity() - buffer.position()
      val n = channel.aRead(buffer, 5000L, TimeUnit.MILLISECONDS)
      if (n == left) throw RuntimeException("Connection buffer too small.")
      if (buffer.position() == 0) return null
      buffer.flip()
      val message = Message.fromBytes(buffer)
      if (message != null) return message
      if (buffer.position() == buffer.capacity()) throw RuntimeException("Connection buffer too small.")
      if (n == -1) return null
    }
  }

  internal suspend fun message(): Message.FromServer? {
    while (true) {
      val message = anyMessage() ?: break
      when (message) {
        is Message.NotificationResponse -> warn(message.toString())
        is Message.NoticeResponse -> warn(message.toString())
        is Message.ParameterStatus -> props[message.key] = message.value
      }
      return message
    }
    return null
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
    /**
     * Connects to a PostgreSQL database using the supplied credentials.
     * @param database the database name.
     * @param credentials the credentials to use for the connection (defaults to postgres unsecured credentials).
     * @param address the server address and port (localhost:5432 by default).
     * @param bufferSize the buffer size (4MB by default).
     */
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
        PostgresAuthentication.authenticate(connection, credentials)
        loop@ while (true) {
          val message = connection.message() ?: throw RuntimeException()
          when (message) {
            is Message.ErrorResponse -> throw RuntimeException(message.toString())
            is Message.ReadyForQuery -> break@loop
            is Message.BackendKeyData ->  {
              val (processId, privateKey) = message
              connection.processId = processId
              connection.privateKey = privateKey
            }
          }
        }
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
