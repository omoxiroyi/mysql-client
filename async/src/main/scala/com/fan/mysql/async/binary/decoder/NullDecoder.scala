

package com.fan.mysql.async.binary.decoder

import io.netty.buffer.ByteBuf

object NullDecoder extends BinaryDecoder {
  def decode(buffer: ByteBuf): Any = null
}
