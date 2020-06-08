

package com.fan.mysql.async.decoder

import com.fan.mysql.async.message.server.ServerMessage
import io.netty.buffer.ByteBuf

trait MessageDecoder {

  def decode(buffer: ByteBuf): ServerMessage

}
