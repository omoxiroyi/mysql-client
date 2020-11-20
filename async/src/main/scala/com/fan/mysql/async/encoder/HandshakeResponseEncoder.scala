package com.fan.mysql.async.encoder

import java.nio.charset.Charset

import com.fan.mysql.async.encoder.auth.AuthenticationMethod
import com.fan.mysql.async.exceptions.UnsupportedAuthenticationMethodException
import com.fan.mysql.async.message.client.{ClientMessage, HandshakeResponseMessage}
import com.fan.mysql.async.util.{ByteBufferUtils, CharsetMapper, Log}
import io.netty.buffer.ByteBuf

object HandshakeResponseEncoder {

  final val MAX_3_BYTES = 0x00ffffff
  final val PADDING: Array[Byte] = List
    .fill(23) {
      0.toByte
    }
    .toArray

  final val log = Log.get[HandshakeResponseEncoder]

}

class HandshakeResponseEncoder(charset: Charset, charsetMapper: CharsetMapper)
    extends MessageEncoder {

  import HandshakeResponseEncoder._
  import com.fan.mysql.async.util.MySQLIO._

  private val authenticationMethods = AuthenticationMethod.Availables

  def encode(message: ClientMessage): ByteBuf = {

    val m = message.asInstanceOf[HandshakeResponseMessage]

    var clientCapabilities = 0

    clientCapabilities |=
      CLIENT_PLUGIN_AUTH |
        CLIENT_PROTOCOL_41 |
        CLIENT_TRANSACTIONS |
        CLIENT_MULTI_RESULTS |
        CLIENT_SECURE_CONNECTION

    if (m.database.isDefined) {
      clientCapabilities |= CLIENT_CONNECT_WITH_DB
    }

    val buffer = ByteBufferUtils.packetBuffer()

    // write capabilities
    buffer.writeInt(clientCapabilities)
    buffer.writeInt(MAX_3_BYTES)

    // write charset
    buffer.writeByte(charsetMapper.toInt(charset))
    buffer.writeBytes(PADDING)
    ByteBufferUtils.writeCString(m.username, buffer, charset)

    // write password
    if (m.password.isDefined) {
      val method = m.authenticationMethod
      val authenticator = this.authenticationMethods.getOrElse(method, {
        throw new UnsupportedAuthenticationMethodException(method)
      })
      val bytes = authenticator.generateAuthentication(charset, m.password, m.seed)
      buffer.writeByte(bytes.length)
      buffer.writeBytes(bytes)
    } else {
      buffer.writeByte(0)
    }

    if (m.database.isDefined) {
      ByteBufferUtils.writeCString(m.database.get, buffer, charset)
    }

    ByteBufferUtils.writeCString(m.authenticationMethod, buffer, charset)

    buffer
  }

}
