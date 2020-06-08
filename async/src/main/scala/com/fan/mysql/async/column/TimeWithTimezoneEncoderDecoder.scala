

package com.fan.mysql.async.column

import org.joda.time.format.DateTimeFormat

object TimeWithTimezoneEncoderDecoder extends TimeEncoderDecoder {

  private val format = DateTimeFormat.forPattern("HH:mm:ss.SSSSSSZ")

  override def formatter = format

}
