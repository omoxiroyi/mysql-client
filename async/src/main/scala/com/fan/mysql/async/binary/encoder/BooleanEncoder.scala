package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf

object BooleanEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf): Unit = {
    val boolean = value.asInstanceOf[Boolean]
    if (boolean) {
      buffer.writeByte(1)
    } else {
      buffer.writeByte(0)
    }
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TINY
}
