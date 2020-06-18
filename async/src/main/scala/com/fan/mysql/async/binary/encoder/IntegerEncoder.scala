

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf

object IntegerEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf): Unit = {
    buffer.writeInt(value.asInstanceOf[Int])
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_LONG
}
