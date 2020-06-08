

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time.LocalDateTime

object SQLTimestampEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf) {
    val date = value.asInstanceOf[java.sql.Timestamp]
    LocalDateTimeEncoder.encode(new LocalDateTime(date.getTime), buffer)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIMESTAMP
}
