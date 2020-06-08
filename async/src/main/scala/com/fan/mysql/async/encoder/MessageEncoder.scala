

package com.fan.mysql.async.encoder

import com.fan.mysql.async.message.client.ClientMessage
import io.netty.buffer.ByteBuf


trait MessageEncoder {

  def encode(message: ClientMessage): ByteBuf

}
