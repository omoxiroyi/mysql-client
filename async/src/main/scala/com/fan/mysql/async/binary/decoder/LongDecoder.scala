

package com.fan.mysql.async.binary.decoder

import io.netty.buffer.ByteBuf

object LongDecoder extends BinaryDecoder {
  def decode(buffer: ByteBuf): Any = buffer.readLong()
}