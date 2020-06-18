


package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf


object ByteArrayEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf): Unit = {
    val bytes = value.asInstanceOf[Array[Byte]]

    buffer.writeLength(bytes.length)
    buffer.writeBytes(bytes)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_BLOB

}
