

package com.fan.mysql.async.binary.encoder

import io.netty.buffer.ByteBuf

trait BinaryEncoder {

  def encode(value: Any, buffer: ByteBuf)

  def encodesTo: Int

}
