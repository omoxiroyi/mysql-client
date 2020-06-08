

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time.LocalTime

object LocalTimeEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf) {
    val time = value.asInstanceOf[LocalTime]

    val hasMillis = time.getMillisOfSecond != 0

    if (hasMillis) {
      buffer.writeByte(12)
    } else {
      buffer.writeByte(8)
    }

    if (time.getMillisOfDay > 0) {
      buffer.writeByte(0)
    } else {
      buffer.writeByte(1)
    }

    buffer.writeInt(0)

    buffer.writeByte(time.getHourOfDay)
    buffer.writeByte(time.getMinuteOfHour)
    buffer.writeByte(time.getSecondOfMinute)

    if (hasMillis) {
      buffer.writeInt(time.getMillisOfSecond * 1000)
    }

  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIME
}
