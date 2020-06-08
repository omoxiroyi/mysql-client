

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf

import scala.concurrent.duration._

object DurationEncoder extends BinaryEncoder {

  private final val Zero = 0.seconds

  def encode(value: Any, buffer: ByteBuf) {
    val duration = value.asInstanceOf[Duration]

    val days = duration.toDays
    val hoursDuration = duration - days.days
    val hours = hoursDuration.toHours
    val minutesDuration = hoursDuration - hours.hours
    val minutes = minutesDuration.toMinutes
    val secondsDuration = minutesDuration - minutes.minutes
    val seconds = secondsDuration.toSeconds
    val microsDuration = secondsDuration - seconds.seconds
    val micros = microsDuration.toMicros

    val hasMicros = micros != 0

    if (hasMicros) {
      buffer.writeByte(12)
    } else {
      buffer.writeByte(8)
    }

    if (duration > Zero) {
      buffer.writeByte(0)
    } else {
      buffer.writeByte(1)
    }

    buffer.writeInt(days.asInstanceOf[Int])
    buffer.writeByte(hours.asInstanceOf[Int])
    buffer.writeByte(minutes.asInstanceOf[Int])
    buffer.writeByte(seconds.asInstanceOf[Int])

    if (hasMicros) {
      buffer.writeInt(micros.asInstanceOf[Int])
    }

  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIME
}
