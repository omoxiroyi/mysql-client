

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time.LocalDate

object LocalDateEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf):Unit = {
    val date = value.asInstanceOf[LocalDate]

    buffer.writeByte(4)
    buffer.writeShort(date.getYear)
    buffer.writeByte(date.getMonthOfYear)
    buffer.writeByte(date.getDayOfMonth)

  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_DATE
}
