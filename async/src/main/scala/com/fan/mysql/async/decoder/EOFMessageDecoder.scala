package com.fan.mysql.async.decoder

import com.fan.mysql.async.message.server.EOFMessage
import io.netty.buffer.ByteBuf

object EOFMessageDecoder extends MessageDecoder {

  def decode(buffer: ByteBuf): EOFMessage = {
    EOFMessage(buffer.readUnsignedShort(), buffer.readUnsignedShort())
  }

}
