

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time.LocalDate

object SQLDateEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf) {
    val date = value.asInstanceOf[java.sql.Date]

    LocalDateEncoder.encode(new LocalDate(date), buffer)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_DATE
}
