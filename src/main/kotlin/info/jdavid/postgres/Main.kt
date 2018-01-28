package info.jdavid.postgres

import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import kotlinx.coroutines.experimental.runBlocking
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.AsynchronousSocketChannel

fun main(args: Array<String>) {
  val username = "postgres"
  val password = "postgres"
  val database = "postgres"
  runBlocking {
    val send = ByteBuffer.allocate(4096).order(ByteOrder.BIG_ENDIAN)
    val receive = ByteBuffer.allocate(4096).order(ByteOrder.BIG_ENDIAN)
    val channel = AsynchronousSocketChannel.open()
    channel.aConnect(InetSocketAddress(InetAddress.getLoopbackAddress(), 5432))

    val ref = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN).let {
      it.putInt(0)
      it.putInt(196608)
      it.put(bytes("user"))
      it.put(0)
      it.put(bytes("postgres"))
      it.put(0)
      it.put(bytes("database"))
      it.put(0)
      it.put(bytes("postgres"))
      it.put(0)
      it.put(0)
      it.putInt(0, send.position())
      it.flip()
      it
    }

    Message.StartupMessage(username, database).writeTo(send)
    send.flip()
    channel.aWrite(send, 0)
    send.clear()
    receive.clear()
    channel.aRead(receive)
    receive.flip()
    Message.PasswordMessage(username, password, Message.fromBytes(receive) as Message.Authentication).
      writeTo(send)
    send.flip()
    channel.aWrite(send, 0)
    send.clear()
    receive.clear()
    channel.aRead(receive)
    receive.flip()
    val ok = Message.fromBytes(receive) as Message.AuthenticationOk
    while (receive.remaining() > 0) {
      println(Message.fromBytes(receive))
    }

    val array = ByteArray(receive.remaining())
    receive.get(array)
    println(String(array, Charsets.US_ASCII).toCharArray().map { c(it) }.joinToString(""))
    println(hex(array))
  }
}

private fun bytes(s: String): ByteArray {
  return s.toByteArray(Charsets.US_ASCII)
}

private fun c(c: Char): String {
  val s = "${c}"
  return when (c) {
    '\u0000' -> "\\0."
    '\n' -> "\\n."
    '\r' -> "\\r."
    '\t' -> "\\t."
    '�' -> "?.."
    else -> null
  } ?: return when (s.length) {
    0 -> "..."
    1 -> "${s}.."
    else -> "?.."
  }
}

private fun hex(bytes: ByteArray): String {
  val chars = CharArray(bytes.size * 3)
  var i = 0
  for (b in bytes) {
    chars[i++] = Character.forDigit(b.toInt().shr(4).and(0xf), 16)
    chars[i++] = Character.forDigit(b.toInt().and(0xf), 16)
    chars[i++] = ' '
  }
  return String(chars)
}
