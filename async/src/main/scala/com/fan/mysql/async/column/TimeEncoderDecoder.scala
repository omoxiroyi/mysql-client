package com.fan.mysql.async.column

import org.joda.time.LocalTime
import org.joda.time.format.DateTimeFormatterBuilder

object TimeEncoderDecoder {
  val Instance = new TimeEncoderDecoder()
}

class TimeEncoderDecoder extends ColumnEncoderDecoder {

  final private val optional = new DateTimeFormatterBuilder()
    .appendPattern(".SSSSSS")
    .toParser

  final private val format = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss")
    .appendOptional(optional)
    .toFormatter

  final private val printer = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss.SSSSSS")
    .toFormatter

  def formatter = format

  override def decode(value: String): LocalTime =
    format.parseLocalTime(value)

  override def encode(value: Any): String =
    this.printer.print(value.asInstanceOf[LocalTime])

}
