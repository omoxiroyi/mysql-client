

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf

object FloatEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf): Unit = {
    buffer.writeFloat(value.asInstanceOf[Float])
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_FLOAT
}
