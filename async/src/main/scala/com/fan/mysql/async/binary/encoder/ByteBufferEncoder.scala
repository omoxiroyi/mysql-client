package com.fan.mysql.async.binary.encoder

import java.nio.ByteBuffer

import com.fan.mysql.async.column.ColumnTypes
import com.fan.mysql.async.util.ChannelWrapper._
import io.netty.buffer.ByteBuf


object ByteBufferEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf) {
    val bytes = value.asInstanceOf[ByteBuffer]

    buffer.writeLength(bytes.remaining())
    buffer.writeBytes(bytes)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_BLOB

}
