

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf

object LongEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf) {
    buffer.writeLong(value.asInstanceOf[Long])
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_LONGLONG
}
