

package com.fan.mysql.async.decoder

import java.nio.charset.Charset

import com.fan.mysql.async.message.server.{OkMessage, ServerMessage}
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf

class OkDecoder(charset: Charset) extends MessageDecoder {

  def decode(buffer: ByteBuf): ServerMessage = {

    OkMessage(
      buffer.readBinaryLength,
      buffer.readBinaryLength,
      buffer.readShort(),
      buffer.readShort(),
      buffer.readUntilEOF(charset)
    )

  }

}
