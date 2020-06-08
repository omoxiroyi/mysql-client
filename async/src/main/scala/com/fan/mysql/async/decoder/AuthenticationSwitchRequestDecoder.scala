package com.fan.mysql.async.decoder

import java.nio.charset.Charset

import com.fan.mysql.async.message.server.{AuthenticationSwitchRequest, ServerMessage}
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf

class AuthenticationSwitchRequestDecoder(charset: Charset) extends MessageDecoder {
  def decode(buffer: ByteBuf): ServerMessage = {
    AuthenticationSwitchRequest(
      buffer.readCString(charset),
      buffer.readUntilEOF(charset)
    )
  }
}
