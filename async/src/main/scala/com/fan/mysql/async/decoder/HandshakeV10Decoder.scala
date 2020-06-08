
package com.fan.mysql.async.decoder

import java.nio.charset.Charset

import com.fan.mysql.async.encoder.auth.AuthenticationMethod
import com.fan.mysql.async.message.server.{HandshakeMessage, ServerMessage}
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.Log
import com.fan.mysql.async.util.MySQLIO._
import io.netty.buffer.ByteBuf
import io.netty.util.CharsetUtil

object HandshakeV10Decoder {
  final val log = Log.get[HandshakeV10Decoder]
  final val SeedSize = 8
  final val SeedComplementSize = 12
  final val Padding = 10
  final val ASCII = CharsetUtil.US_ASCII
}

class HandshakeV10Decoder(charset: Charset) extends MessageDecoder {

  import HandshakeV10Decoder._

  def decode(buffer: ByteBuf): ServerMessage = {

    val serverVersion = buffer.readCString(ASCII)
    val connectionId = buffer.readUnsignedInt()

    val seed = new Array[Byte](SeedSize + SeedComplementSize)
    buffer.readBytes(seed, 0, SeedSize)

    buffer.readByte() // filler

    // read capability flags (lower 2 bytes)
    var serverCapabilityFlags = buffer.readUnsignedShort()

    /* New protocol with 16 bytes to describe server characteristics */
    // read character set (1 byte)
    val characterSet = buffer.readByte() & 0xff
    // read status flags (2 bytes)
    val statusFlags = buffer.readUnsignedShort()

    // read capability flags (upper 2 bytes)
    serverCapabilityFlags |= buffer.readUnsignedShort() << 16

    var authPluginDataLength = 0
    var authenticationMethod: String = AuthenticationMethod.Native

    if ((serverCapabilityFlags & CLIENT_PLUGIN_AUTH) != 0) {
      // read length of auth-plugin-data (1 byte)
      authPluginDataLength = buffer.readByte() & 0xff
    } else {
      // read filler ([00])
      buffer.readByte()
    }

    // next 10 bytes are reserved (all [00])
    buffer.readerIndex(buffer.readerIndex() + Padding)

    log.debug(s"Auth plugin data length was $authPluginDataLength")

    if ((serverCapabilityFlags & CLIENT_SECURE_CONNECTION) != 0) {
      val complement = if (authPluginDataLength > 0) {
        authPluginDataLength - 1 - SeedSize
      } else {
        SeedComplementSize
      }

      buffer.readBytes(seed, SeedSize, complement)
      buffer.readByte()
    }

    if ((serverCapabilityFlags & CLIENT_PLUGIN_AUTH) != 0) {
      authenticationMethod = buffer.readUntilEOF(ASCII)
    }

    val message = HandshakeMessage(
      serverVersion,
      connectionId,
      seed,
      serverCapabilityFlags,
      characterSet = characterSet,
      statusFlags = statusFlags,
      authenticationMethod = authenticationMethod
    )

    log.debug(s"handshake message was $message")

    message
  }

}