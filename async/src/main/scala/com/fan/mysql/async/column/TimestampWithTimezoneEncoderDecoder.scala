package com.fan.mysql.async.column

import org.joda.time.format.DateTimeFormat

object TimestampWithTimezoneEncoderDecoder extends TimestampEncoderDecoder {

  private val format = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSZ")

  override def formatter = format

  override def decode(value: String): Any = {
    formatter.parseDateTime(value)
  }

}
