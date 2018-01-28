package info.jdavid.postgres

import java.nio.ByteBuffer
import java.security.MessageDigest

sealed class Message {

  interface Authentication

  internal interface FromServer
  internal interface FromClient {
    fun writeTo(buffer: ByteBuffer)
  }

  class StartupMessage(private val username: String, private val database: String): FromClient, Message() {
    override fun writeTo(buffer: ByteBuffer) {
      val start = buffer.position()
      buffer.putInt(0)
      buffer.putInt(196608)
      buffer.put("user".toByteArray())
      buffer.put(0)
      buffer.put(username.toByteArray())
      buffer.put(0)
      buffer.put("database".toByteArray())
      buffer.put(0)
      buffer.put(database.toByteArray())
      buffer.put(0)
      buffer.put(0)
      buffer.putInt(start, buffer.position() - start)
    }
  }

  class AuthenticationOk: FromServer, Authentication, Message() {
    override fun toString() = "AuthenticationOk"
  }
  class AuthenticationKerberosV5: FromServer, Authentication, Message() {
    override fun toString() = "AuthenticationKerberosV5"
  }
  class AuthenticationCleartextPassword: FromServer, Authentication, Message() {
    override fun toString() = "AuthenticationCleartextPassword"
  }
  class AuthenticationMD5Password(val salt: ByteArray): FromServer, Authentication, Message() {
    override fun toString() = "AuthenticationMd5Password(salt=${hex(salt)})"
  }
  class AuthenticationSCMCredential: FromServer, Authentication, Message() {
    override fun toString() = "AuthenticationSCMCredential"
  }
  class AuthenticationGSS: FromServer, Authentication, Message() {
    override fun toString() = "AuthenticationGSS"
  }
  class AuthenticationGSSContinue(val auth: ByteArray): FromServer, Authentication, Message() {
    override fun toString() = "AuthenticationGSSContinue"
  }
  class AuthenticationSSPI: FromServer, Authentication, Message() {
    override fun toString() = "AuthenticationSSPI"
  }

  class ParameterStatus(val key: String, val value: String): FromServer, Message() {
    override fun toString() = "ParameterStatus(key: ${key}, value: ${value})"
  }

  class BackendKeyData(val processId: Int, val secretKey: Int): FromServer, Message() {
    override fun toString() = "BackendKeyData(processId: ${processId}, secretKey: ${secretKey})"
  }

  class ReadyForQuery(val status: Status): FromServer, Message() {
    override fun toString() = "ReadyForQuery(status: ${status})"
    enum class Status {
      IDLE, IN_TRANSACTION, IN_FAILED_TRANSACTION;
      companion object {
        internal fun from(b: Byte): Status {
          return when (b) {
            'I'.toByte() -> IDLE
            'T'.toByte() -> IN_TRANSACTION
            'E'.toByte() -> IN_FAILED_TRANSACTION
            else -> throw IllegalArgumentException()
          }
        }
      }
    }
  }

  class ErrorResponse(private val message: String): FromServer, Message() {
    override fun toString() = "ErrorResponse(){\n${message}\n}"
  }

  //--------------------------------------------------------------------------------------------------

  class PasswordMessage(val username: String, val password: String,
                        val authMessage: Authentication): FromClient, Message() {
    override fun toString() = "PasswordMessage(username: ${username})"
    override fun writeTo(buffer: ByteBuffer) {
      when (authMessage) {
        is AuthenticationCleartextPassword -> {
          buffer.put('p'.toByte())
          val start = buffer.position()
          buffer.putInt(0)
          buffer.put(password.toByteArray())
          buffer.put(0)
          buffer.putInt(start, buffer.position() - start)
        }
        is AuthenticationMD5Password -> {
          val md5 = md5(username, password, authMessage.salt)
          buffer.put('p'.toByte())
          val start = buffer.position()
          buffer.putInt(0)
          buffer.put(md5)
          buffer.put(0)
          buffer.putInt(start, buffer.position() - start)
        }
      }
    }
  }



  companion object {
    @Suppress("UsePropertyAccessSyntax")
    fun fromBytes(buffer: ByteBuffer): Message {
      val first = buffer.get()
      when (first) {
        'R'.toByte() -> { // authentication
          val length = buffer.getInt()
          val flag = buffer.getInt()
          when (flag) {
            0 -> {
              assert(length == 8)
              return AuthenticationOk()
            }
            2 -> {
              assert(length == 8)
              return AuthenticationKerberosV5()
            }
            3 -> {
              assert(length == 8)
              return AuthenticationCleartextPassword()
            }
            5 -> {
              assert(length == 12)
              val salt = ByteArray(4)
              buffer.get(salt)
              return AuthenticationMD5Password(salt)
            }
            6 -> {
              assert(length == 8)
              return AuthenticationSCMCredential()
            }
            7 -> {
              assert(length == 8)
              return AuthenticationGSS()
            }
            8 -> {
              assert(length == 8)
              return AuthenticationSSPI()
            }
            9 -> {
              val size = length - 8
              val auth = ByteArray(size)
              buffer.get(auth)
              return AuthenticationGSSContinue(auth)
            }
            else -> throw IllegalArgumentException()
          }
        }
        'S'.toByte() -> {
          val length = buffer.getInt()
          val data = ByteArray(length - 4)
          buffer.get(data)
          val i = data.indexOf(0)
          val key = String(data, 0, i)
          val value = String(data, i + 1, data.size - i - 2)
          return ParameterStatus(key, value)
        }
        'K'.toByte() -> {
          val length = buffer.getInt()
          assert(length == 12)
          val processId = buffer.getInt()
          val secretKey = buffer.getInt()
          return BackendKeyData(processId, secretKey)
        }
        'Z'.toByte() -> {
          val length = buffer.getInt()
          assert(length == 5)
          val status = ReadyForQuery.Status.from(buffer.get())
          return ReadyForQuery(status)
        }
        'E'.toByte() -> {
          val length = buffer.getInt()
          val data = ByteArray(length - 4)
          buffer.get(data)
          val message = StringBuilder()
          var sb = StringBuilder()
          for (b in data) {
            if (sb.isEmpty()) {
              when (b) {
                'S'.toByte() -> message.append("SEVERITY: ")
                'C'.toByte() -> message.append("SQLSTATE ERROR CODE: ")
                'M'.toByte() -> message.append("MESSAGE: ")
                'D'.toByte() -> message.append("DETAIL: ")
                'P'.toByte() -> message.append("POSITION: ")
                'p'.toByte() -> message.append("INTERNAL POSITION: ")
                'q'.toByte() -> message.append("INTERNAL QUERY: ")
                'W'.toByte() -> message.append("WHERE: ")
                's'.toByte() -> message.append("SCHEMA NAME: ")
                't'.toByte() -> message.append("TABLE NAME: ")
                'c'.toByte() -> message.append("COLUMN NAME: ")
                'd'.toByte() -> message.append("DATA TYPE NAME: ")
                'n'.toByte() -> message.append("CONSTRAINT NAME: ")
                'F'.toByte() -> message.append("FILE: ")
                'L'.toByte() -> message.append("LINE: ")
                'R'.toByte() -> message.append("ROUTINE: ")
              }
            }
            else {
              if (b == 0.toByte()) {
                message.append(sb)
                message.append('\n')
                sb = StringBuilder()
              }
            }
          }
          return ErrorResponse(message.toString())
        }
        else -> throw IllegalArgumentException()
      }
    }

    private fun md5(username: String, password: String, salt: ByteArray): ByteArray {
      val md5 = MessageDigest.getInstance("MD5")
      md5.update(password.toByteArray())
      md5.update(username.toByteArray())
      val hash = hex(md5.digest()).toByteArray()
      md5.update(hash)
      md5.update(salt)
      return "md5${hex(md5.digest())}".toByteArray()
    }

    private fun hex(bytes: ByteArray): String {
      val chars = CharArray(bytes.size * 2)
      var i = 0
      for (b in bytes) {
        chars[i++] = Character.forDigit(b.toInt().shr(4).and(0xf), 16)
        chars[i++] = Character.forDigit(b.toInt().and(0xf), 16)
      }
      return String(chars)
    }

  }

}
