

package com.fan.mysql.async.binary.decoder

import io.netty.buffer.ByteBuf

object ShortDecoder extends BinaryDecoder {
  def decode(buffer: ByteBuf): Any = buffer.readShort()
}
