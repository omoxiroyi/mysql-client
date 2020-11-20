package com.fan.mysql.async.binary.decoder

import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf

object ByteArrayDecoder extends BinaryDecoder {
  def decode(buffer: ByteBuf): Any = {
    val length = buffer.readBinaryLength
    val bytes  = new Array[Byte](length.toInt)
    buffer.readBytes(bytes)

    bytes
  }
}
