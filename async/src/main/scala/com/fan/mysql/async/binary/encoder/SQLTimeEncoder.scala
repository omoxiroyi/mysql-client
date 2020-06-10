

package com.fan.mysql.async.binary.encoder

import com.fan.mysql.async.column.ColumnTypes
import io.netty.buffer.ByteBuf
import org.joda.time.LocalTime

object SQLTimeEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf): Unit = {
    val sqlTime = value.asInstanceOf[java.sql.Time].getTime
    val time = new LocalTime(sqlTime)
    LocalTimeEncoder.encode(time, buffer)
  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIME
}
