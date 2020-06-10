

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf

object ByteEncoder extends BinaryEncoder {

  def encode(value: Any, buffer: ByteBuf):Unit = {
    buffer.writeByte(value.asInstanceOf[Byte])
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TINY
}
