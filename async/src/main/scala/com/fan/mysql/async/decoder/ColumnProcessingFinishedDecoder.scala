

package com.fan.mysql.async.decoder

import com.fan.mysql.async.message.server.{ColumnProcessingFinishedMessage, ServerMessage}
import io.netty.buffer.ByteBuf

object ColumnProcessingFinishedDecoder extends MessageDecoder {

  def decode(buffer: ByteBuf): ServerMessage = {
    ColumnProcessingFinishedMessage(EOFMessageDecoder.decode(buffer))
  }

}
