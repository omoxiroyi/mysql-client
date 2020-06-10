

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf

object ShortEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf):Unit = {
    buffer.writeShort(value.asInstanceOf[Short])
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_SHORT
}
