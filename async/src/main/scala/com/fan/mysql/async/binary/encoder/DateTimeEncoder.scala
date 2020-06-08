

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time._

object DateTimeEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf) {
    val instant = value.asInstanceOf[ReadableDateTime]

    LocalDateTimeEncoder.encode(new LocalDateTime(instant.getMillis), buffer)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIMESTAMP

}
