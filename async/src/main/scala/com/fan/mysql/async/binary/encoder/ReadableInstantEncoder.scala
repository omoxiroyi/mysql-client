package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time._

object ReadableInstantEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf): Unit = {
    val date = value.asInstanceOf[ReadableInstant]
    LocalDateTimeEncoder.encode(new LocalDateTime(date.getMillis), buffer)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIMESTAMP
}
