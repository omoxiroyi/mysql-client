package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf

object DoubleEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf): Unit = {
    buffer.writeDouble(value.asInstanceOf[Double])
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_DOUBLE
}
