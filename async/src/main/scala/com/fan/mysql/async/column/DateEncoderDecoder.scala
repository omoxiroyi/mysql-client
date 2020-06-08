

package com.fan.mysql.async.column

import com.fan.mysql.async.exceptions.DateEncoderNotAvailableException
import org.joda.time.format.DateTimeFormat
import org.joda.time.{LocalDate, ReadablePartial}

object DateEncoderDecoder extends ColumnEncoderDecoder {

  private val ZeroedDate = "0000-00-00"

  private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  override def decode(value: String): LocalDate =
    if (ZeroedDate == value) {
      null
    } else {
      this.formatter.parseLocalDate(value)
    }

  override def encode(value: Any): String = {
    value match {
      case d: java.sql.Date => this.formatter.print(new LocalDate(d))
      case d: ReadablePartial => this.formatter.print(d)
      case _ => throw new DateEncoderNotAvailableException(value)
    }
  }

}
