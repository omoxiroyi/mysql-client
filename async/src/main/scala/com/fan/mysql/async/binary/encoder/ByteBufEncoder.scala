package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf


object ByteBufEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf) {
    val bytes = value.asInstanceOf[ByteBuf]

    buffer.writeLength(bytes.readableBytes())
    buffer.writeBytes(bytes)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_BLOB

}
