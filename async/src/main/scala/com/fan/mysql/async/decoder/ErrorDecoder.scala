package com.fan.mysql.async.decoder

import java.nio.charset.Charset

import com.fan.mysql.async.message.server.{ErrorMessage, ServerMessage}
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf

class ErrorDecoder(charset: Charset) extends MessageDecoder {

  def decode(buffer: ByteBuf): ServerMessage = {

    ErrorMessage(
      buffer.readShort(),
      buffer.readFixedString(6, charset),
      buffer.readUntilEOF(charset)
    )

  }

}
