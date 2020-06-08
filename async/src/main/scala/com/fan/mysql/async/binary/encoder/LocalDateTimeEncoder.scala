

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time._

object LocalDateTimeEncoder extends BinaryEncoder {

  def encode(value: Any, buffer: ByteBuf) {
    val instant = value.asInstanceOf[LocalDateTime]

    val hasMillis = instant.getMillisOfSecond != 0

    if (hasMillis) {
      buffer.writeByte(11)
    } else {
      buffer.writeByte(7)
    }

    buffer.writeShort(instant.getYear)
    buffer.writeByte(instant.getMonthOfYear)
    buffer.writeByte(instant.getDayOfMonth)
    buffer.writeByte(instant.getHourOfDay)
    buffer.writeByte(instant.getMinuteOfHour)
    buffer.writeByte(instant.getSecondOfMinute)

    if (hasMillis) {
      buffer.writeInt(instant.getMillisOfSecond * 1000)
    }

  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIMESTAMP
}
