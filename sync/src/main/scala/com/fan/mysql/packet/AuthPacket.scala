package com.fan.mysql.packet

import java.io.UnsupportedEncodingException
import java.security.NoSuchAlgorithmException

import com.fan.mysql.config.Capabilities
import com.fan.mysql.util.{MySQLPacketBuffer, SecurityUtil}

/**
 * <pre>
 * 4              capability flags, CLIENT_PROTOCOL_41 always set
 * 4              max-packet size
 * 1              character set
 * string[23]     reserved (all [0])
 * string[NUL]    username
 * if capabilities & CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA {
 * lenenc-int     length of auth-response
 * string[n]      auth-response
 * } else if capabilities & CLIENT_SECURE_CONNECTION {
 * 1              length of auth-response
 * string[n]      auth-response
 * } else {
 * string[NUL]    auth-response
 * }
 * if capabilities & CLIENT_CONNECT_WITH_DB {
 * string[NUL]    database
 * }
 * if capabilities & CLIENT_PLUGIN_AUTH {
 * string[NUL]    auth plugin name
 * }
 * if capabilities & CLIENT_CONNECT_ATTRS {
 * lenenc-int     length of all key-values
 * lenenc-str     key
 * lenenc-str     value
 * if-more data in 'length of all key-values', more keys and value pairs
 * }
 * </pre>
 *
 * @author fan
 */
//noinspection DuplicatedCode
class AuthPacket extends MySQLPacket {
  var clientFlags: Long = _
  var maxPacketSize: Long = _
  var charsetIndex: Int = _
  var user: String = _
  var seed: String = _
  var password: String = _
  // 16 bytes
  var encryptedPassword: Array[Byte] = _
  var database: String = _
  var authPluginName: String = _

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    this.clientFlags = buffer.readUB4
    this.maxPacketSize = buffer.readUB4
    this.charsetIndex = buffer.read & 0xff
    buffer.move(23)
    this.user = buffer.readStringWithNull
    if ((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA) != 0) {
      val len = buffer.readLength.toInt
      this.encryptedPassword = buffer.readBytes(len)
    }
    else if ((clientFlags & Capabilities.CLIENT_SECURE_CONNECTION) != 0) {
      val len = buffer.read
      this.encryptedPassword = buffer.readBytes(len)
    }
    else this.encryptedPassword = buffer.readBytesWithNull
    if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) && buffer.hasRemaining)
      this.database = buffer.readStringWithNull
    if (((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) && buffer.hasRemaining)
      this.authPluginName = buffer.readStringWithNull
    if (((clientFlags & Capabilities.CLIENT_CONNECT_ATTRS) != 0) && buffer.hasRemaining) {
      // ignore..
    }
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    buffer.writeUB4(this.clientFlags)
    buffer.writeUB4(this.maxPacketSize)
    buffer.write(this.charsetIndex.toByte)
    buffer.writeBytesNoNull(new Array[Byte](23))
    buffer.writeStringWithNull(this.user)
    if (this.password == null) buffer.write(0.toByte)
    else {
      getEncryptedPassword
      buffer.writeLength(encryptedPassword.length)
      buffer.writeBytesNoNull(encryptedPassword)
    }
    buffer.writeStringWithNull(this.database)
    if ((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) buffer.writeStringWithNull(authPluginName)
  }

  override def calcPacketSize: Int = {
    var size = super.calcPacketSize
    size += 32 // 4+4+1+23;

    try {
      size +=
        (if (user == null) 1
        else if (charset == null) user.getBytes.length + 1
        else user.getBytes(charset).length + 1)
      size +=
        (if (password == null) 1
        else MySQLPacketBuffer.getLength(getEncryptedPassword))
      size +=
        (if (database == null) 1
        else if (charset == null) database.getBytes.length + 1
        else database.getBytes(charset).length + 1)
      if ((clientFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
        size +=
          (if (authPluginName == null) 1
          else if (charset == null) authPluginName.getBytes.length + 1
          else authPluginName.getBytes(charset).length + 1)
      }
    } catch {
      case _: UnsupportedEncodingException =>

    }
    size
  }

  private def getEncryptedPassword = {
    try if (this.encryptedPassword == null)
      this.encryptedPassword = SecurityUtil.scramble411(password, this.seed)
    catch {
      case e: NoSuchAlgorithmException =>
        throw new IllegalArgumentException(e.getMessage)
    }
    this.encryptedPassword
  }

  override def getPacketInfo = "MySQL Authentication Packet"

}
