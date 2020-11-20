package com.fan.mysql.async.binary.decoder

import io.netty.buffer.ByteBuf

trait BinaryDecoder {

  def decode(buffer: ByteBuf): Any

}
