

package com.fan.mysql.async.encoder

import com.fan.mysql.async.message.client.ClientMessage
import com.fan.mysql.async.util.ByteBufferUtils
import io.netty.buffer.ByteBuf

object QuitMessageEncoder extends MessageEncoder {

  def encode(message: ClientMessage): ByteBuf = {
    val buffer = ByteBufferUtils.packetBuffer(5)
    buffer.writeByte(ClientMessage.Quit)
    buffer
  }

}
