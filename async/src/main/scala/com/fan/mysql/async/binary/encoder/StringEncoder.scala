

package com.fan.mysql.async.binary.encoder

import java.nio.charset.Charset

import com.fan.mysql.async.column.ColumnTypes
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.Log
import io.netty.buffer.ByteBuf

object StringEncoder {
  final val log = Log.get[StringEncoder]
}

class StringEncoder(charset: Charset) extends BinaryEncoder {

  def encode(value: Any, buffer: ByteBuf): Unit = {
    buffer.writeLengthEncodedString(value.toString, charset)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_VARCHAR

}