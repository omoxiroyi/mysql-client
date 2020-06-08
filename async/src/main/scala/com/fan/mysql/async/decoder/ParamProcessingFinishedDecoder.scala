

package com.fan.mysql.async.decoder

import com.fan.mysql.async.message.server.{ParamProcessingFinishedMessage, ServerMessage}
import io.netty.buffer.ByteBuf

object ParamProcessingFinishedDecoder extends MessageDecoder {

  def decode(buffer: ByteBuf): ServerMessage = {
    ParamProcessingFinishedMessage(EOFMessageDecoder.decode(buffer))
  }

}
