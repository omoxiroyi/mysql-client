

package com.fan.mysql.async.decoder

import com.fan.mysql.async.message.server.{ParamAndColumnProcessingFinishedMessage, ServerMessage}
import io.netty.buffer.ByteBuf

object ParamAndColumnProcessingFinishedDecoder extends MessageDecoder {
  def decode(buffer: ByteBuf): ServerMessage = {
    ParamAndColumnProcessingFinishedMessage(EOFMessageDecoder.decode(buffer))
  }
}
