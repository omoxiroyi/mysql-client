package com.fan.mysql.async.column

import org.joda.time.LocalTime
import org.joda.time.format.DateTimeFormatterBuilder

object SQLTimeEncoder extends ColumnEncoder {

  final private val format = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss")
    .toFormatter

  override def encode(value: Any): String = {
    val time = value.asInstanceOf[java.sql.Time]

    format.print(new LocalTime(time.getTime))
  }
}
