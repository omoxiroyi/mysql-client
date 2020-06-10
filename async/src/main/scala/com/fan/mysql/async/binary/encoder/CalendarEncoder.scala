

package com.fan.mysql.async.binary.encoder

import java.util.Calendar

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time.LocalDateTime

object CalendarEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf):Unit = {
    val calendar = value.asInstanceOf[Calendar]
    LocalDateTimeEncoder.encode(new LocalDateTime(calendar.getTimeInMillis), buffer)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIMESTAMP

}
