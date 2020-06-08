

package com.fan.mysql.async.binary.decoder

import java.nio.charset.Charset

import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf

class BigDecimalDecoder(charset: Charset) extends BinaryDecoder {
  def decode(buffer: ByteBuf): Any = {
    BigDecimal(buffer.readLengthEncodedString(charset))
  }
}
