

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time.LocalDateTime

object JavaDateEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf):Unit = {
    val date = value.asInstanceOf[java.util.Date]
    LocalDateTimeEncoder.encode(new LocalDateTime(date.getTime), buffer)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIMESTAMP
}
