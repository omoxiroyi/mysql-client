

package com.fan.mysql.async.binary.decoder

import java.nio.charset.Charset

import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.Log
import io.netty.buffer.ByteBuf


object StringDecoder {
  final val log = Log.get[StringDecoder]
}

class StringDecoder(charset: Charset) extends BinaryDecoder {

  def decode(buffer: ByteBuf): Any = {
    buffer.readLengthEncodedString(charset)
  }
}
