

package com.fan.mysql.async.decoder

import java.nio.charset.Charset

import com.fan.mysql.async.message.server.{ResultSetRowMessage, ServerMessage}
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf

object ResultSetRowDecoder {

  final val NULL = 0xfb

}

class ResultSetRowDecoder(charset: Charset) extends MessageDecoder {

  import ResultSetRowDecoder.NULL

  def decode(buffer: ByteBuf): ServerMessage = {
    val row = new ResultSetRowMessage()

    while (buffer.isReadable()) {
      if (buffer.getUnsignedByte(buffer.readerIndex()) == NULL) {
        buffer.readByte()
        row += null
      } else {
        val length = buffer.readBinaryLength.asInstanceOf[Int]
        row += buffer.readBytes(length)
      }
    }

    row
  }
}
